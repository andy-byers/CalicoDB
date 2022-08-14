#ifndef CALICO_WAL_HELPERS_H
#define CALICO_WAL_HELPERS_H

#include <mutex>
#include "basic_wal.h"
#include "calico/store.h"
#include "utils/queue.h"
#include "utils/types.h"
#include "utils/scratch.h"

namespace calico {

constexpr auto WAL_PREFIX = "wal";

struct SegmentId
    : public NullableId<SegmentId>,
      public EqualityComparableTraits<SegmentId>,
      public OrderableTraits<SegmentId>
{
    static constexpr auto NAME_FORMAT = "{}-{:06d}";
    static constexpr Size DIGITS_SIZE {6};
    using Hash = IndexHash<SegmentId>;

    constexpr SegmentId() noexcept = default;

    template<class U>
    constexpr explicit SegmentId(U u) noexcept
        : value {std::uint64_t(u)}
    {}

    [[nodiscard]]
    static auto from_name(BytesView name) -> SegmentId
    {
        if (name.size() < DIGITS_SIZE)
            return null();

        auto digits = name.advance(name.size() - DIGITS_SIZE);

        // Don't call std::stoul() if it's going to throw an exception.
        const auto is_valid = std::all_of(digits.data(), digits.data() + digits.size(), [](auto c) {return std::isdigit(c);});

        if (!is_valid)
            return null();

        return SegmentId {std::stoull(btos(digits))};
    }

    [[nodiscard]]
    auto to_name() const -> std::string
    {
        return fmt::format(NAME_FORMAT, WAL_PREFIX, value);
    }

    constexpr explicit operator std::uint64_t() const
    {
        return value;
    }

    std::uint64_t value {};
};

struct BlockNumber: public EqualityComparableTraits<SegmentId> {
    using Hash = IndexHash<BlockNumber>;

    constexpr BlockNumber() noexcept = default;

    template<class U>
    constexpr explicit BlockNumber(U u) noexcept
        : value {std::uint64_t(u)}
    {}

    constexpr explicit operator std::uint64_t() const
    {
        return value;
    }

    std::uint64_t value {};
};

struct BlockOffset: public EqualityComparableTraits<SegmentId> {
    using Hash = IndexHash<BlockNumber>;

    constexpr BlockOffset() noexcept = default;

    template<class U>
    constexpr explicit BlockOffset(U u) noexcept
        : value {std::uint64_t(u)}
    {}

    constexpr explicit operator std::uint64_t() const
    {
        return value;
    }

    std::uint64_t value {};
};

struct LogPosition {
    BlockNumber number;
    BlockOffset offset;
};

class LogHelper {
public:
    using Action = std::function<Status()>;

    explicit LogHelper(Size block_size, Action action)
        : m_block(block_size, '\x00'),
          m_action {std::move(action)}
    {}

    virtual ~LogHelper() = default;

    [[nodiscard]]
    auto position() const -> LogPosition
    {
        return m_position;
    }

    [[nodiscard]]
    auto remaining() -> Bytes
    {
        return stob(m_block).advance(m_position.offset.value);
    }

    [[nodiscard]]
    auto block() -> Bytes
    {
        return stob(m_block);
    }

    [[nodiscard]]
    auto block() const -> BytesView
    {
        return stob(m_block);
    }

    auto reset() -> void
    {
        m_position.number.value = 0;
        m_position.offset.value = 0;
    }

    auto advance_cursor(Size record_size) -> void
    {
        const auto bytes_remaining = m_block.size() - m_position.offset.value;
        CALICO_EXPECT_LE(record_size, bytes_remaining);
        m_position.offset.value += record_size;
    }

    [[nodiscard]]
    auto advance_block() -> Status
    {
        auto s = m_action();
        if (s.is_ok()) {
            m_position.number.value++;
            m_position.offset.value = 0;
        }
        return s;
    }

private:
    std::string m_block;
    Action m_action;
    LogPosition m_position;
};

class SequentialLogReader final: public LogHelper {
public:
    explicit SequentialLogReader(Size block_size)
        : LogHelper {block_size, [this] {
              const auto offset = (position().number.value+1) * block().size();
              return read_exact(*m_file, block(), offset);
          }}
    {}

    ~SequentialLogReader() override = default;

    [[nodiscard]]
    auto is_attached() const -> bool
    {
        return m_file != nullptr;
    }

    [[nodiscard]]
    auto attach(RandomReader *file) -> Status
    {
        m_file.reset(file);
        reset();
        return read_exact(*file, remaining(), 0);
    }

    [[nodiscard]]
    auto detach() -> RandomReader*
    {
        return m_file.release();
    }

private:
    std::unique_ptr<RandomReader> m_file;
};

class RandomLogReader final {
public:
    explicit RandomLogReader(Size block_size)
        : m_tail(block_size, '\x00')
    {}

    ~RandomLogReader() = default;

    [[nodiscard]]
    auto is_attached() const -> bool
    {
        return m_file != nullptr;
    }

    [[nodiscard]]
    auto attach(RandomReader *file) -> Status
    {
        m_file.reset(file);
        m_block_num.value = 0;
        // Read the first block when read() is called.
        m_has_block = false;
        return Status::ok();
    }

    [[nodiscard]]
    auto detach() -> RandomReader*
    {
        return m_file.release();
    }

    auto read(LogPosition position, Bytes &out) -> Status
    {
        auto tail = stob(m_tail);
        if (!m_has_block || position.number != m_block_num) {
            auto s = read_exact(*m_file, tail, position.number.value * m_tail.size());

            // If s is not OK, the tail buffer has an indeterminate state. We'll have to reread it next time.
            if (!s.is_ok()) {
                m_has_block = false;
                return s;
            }
        }
        out = tail.advance(position.offset.value);
        return Status::ok();
    }

private:
    std::unique_ptr<RandomReader> m_file;
    std::string m_tail;
    BlockNumber m_block_num;
    bool m_has_block {};
};

class AppendLogWriter final: public LogHelper {
public:

    explicit AppendLogWriter(Size block_size)
        : LogHelper {block_size, [this] {
              // Clear unused bytes at the end of the tail buffer.
              mem_clear(remaining());
              return m_file->write(block());
          }}
    {}

    ~AppendLogWriter() override = default;

    [[nodiscard]]
    auto is_attached() const -> bool
    {
        return m_file != nullptr;
    }

    [[nodiscard]]
    auto attach(AppendWriter *file) -> Status
    {
        m_file.reset(file);
        return Status::ok();
    }

    [[nodiscard]]
    auto detach() -> AppendWriter*
    {
        return m_file.release();
    }

private:
    std::unique_ptr<AppendWriter> m_file;
};

class LogSegment {
public:
    explicit LogSegment(std::string path)
        : m_path {std::move(path)},
          m_id {SegmentId::from_name(stob(m_path))}
    {}

    auto add_position(LogPosition position) -> void
    {
        m_positions.emplace_back(position);
    }

    [[nodiscard]]
    auto positions() const -> const std::vector<LogPosition>&
    {
        return m_positions;
    }

private:
    std::vector<LogPosition> m_positions;
    std::string m_path;
    SegmentId m_id;
};

/**
 * Provides fixed-length scratch buffers for WAL payload data with synchronization.
 *
 * Used to pass scratch memory between the WAL component and background writer thread.
 */
class LogScratchManager final {
public:
    explicit LogScratchManager(Size buffer_size)
        : m_manager {buffer_size}
    {}

    ~LogScratchManager() = default;

    [[nodiscard]]
    auto get() -> ManualScratch
    {
        std::lock_guard lock {m_mutex};
        return m_manager.get();
    }

    auto put(ManualScratch scratch) -> void
    {
        std::lock_guard lock {m_mutex};
        m_manager.put(scratch);
    }

private:
    mutable std::mutex m_mutex;
    ManualScratchManager m_manager;
};

class LogEventQueue final {
public:
    struct Event {

    };

private:
    Queue<Event> m_queue;
};

} // namespace calico

#endif // CALICO_WAL_HELPERS_H
