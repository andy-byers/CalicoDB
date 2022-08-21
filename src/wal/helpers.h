#ifndef CALICO_WAL_HELPERS_H
#define CALICO_WAL_HELPERS_H

#include <map>
#include <mutex>
#include "record.h"
#include "calico/store.h"
#include "utils/logging.h"
#include "utils/queue.h"
#include "utils/types.h"
#include "utils/scratch.h"

namespace calico {

class WalBuffer final {
public:
    explicit WalBuffer(Size size)
        : m_buffer(size, '\x00')
    {
        CALICO_EXPECT_TRUE(is_power_of_two(size));
    }

    ~WalBuffer() = default;

    [[nodiscard]]
    auto block_number() const -> Size
    {
        return m_number;
    }

    [[nodiscard]]
    auto block_offset() const -> Size
    {
        return m_offset;
    }

    [[nodiscard]]
    auto remaining() -> Bytes
    {
        return stob(m_buffer).advance(block_offset());
    }

    [[nodiscard]]
    auto remaining() const -> BytesView
    {
        return stob(m_buffer).advance(block_offset());
    }

    [[nodiscard]]
    auto block() -> Bytes
    {
        return stob(m_buffer);
    }

    [[nodiscard]]
    auto buffer() const -> BytesView
    {
        return stob(m_buffer);
    }

    auto advance_cursor(Size record_size) -> void
    {
        CALICO_EXPECT_LE(record_size, m_buffer.size() - m_offset);
        m_offset += record_size;
    }

    template<class Advance>
    [[nodiscard]]
    auto advance_block(const Advance &advance) -> Status
    {
        auto s = advance();
        if (s.is_ok()) {
            m_offset = 0;
            m_number++;
        }
        return s;
    }

    auto reset() -> void
    {
        m_number = 0;
        m_offset = 0;
    }

protected:
    std::string m_buffer;
    Size m_number {};
    Size m_offset {};
};

struct WalMetadata {
    SequenceId first_lsn;
    bool has_commit {};
};

class WalCollection final {
public:
    WalCollection() = default;
    ~WalCollection() = default;

    [[nodiscard]]
    auto is_segment_started() const -> bool
    {
        return !m_id.is_null();
    }

    auto start_segment(SegmentId id, SequenceId first_lsn) -> void
    {
        CALICO_EXPECT_FALSE(is_segment_started());
        m_id = id;
        m_meta.first_lsn = first_lsn;
        m_meta.has_commit = false;
    }

    auto abort_segment() -> void
    {
        CALICO_EXPECT_TRUE(is_segment_started());
        m_id = SegmentId::null();
        m_meta.first_lsn = SequenceId::null();
        m_meta.has_commit = false;
    }

    auto finish_segment(bool has_commit) -> void
    {
        CALICO_EXPECT_TRUE(is_segment_started());
        m_meta.has_commit = has_commit;
        m_segments.emplace(m_id, m_meta);
        abort_segment();
    }

    template<class Action>
    [[nodiscard]]
    auto remove_segments_from_left(SegmentId id, const Action &action) -> Status
    {
        // Removes segments from [<begin>, id).
        while (!m_segments.empty()) {
            const auto itr = cbegin(m_segments);
            if (id == itr->first) return Status::ok();
            auto s = action(itr->first, itr->second);
            if (!s.is_ok()) return s;
            m_segments.erase(itr);
        }
        return Status::ok();
    }

    template<class Action>
    [[nodiscard]]
    auto remove_segments_from_right(SegmentId id, const Action &action) -> Status
    {
        // Removes segments from [id, <end>) in reverse.
        while (!m_segments.empty()) {
            const auto itr = prev(cend(m_segments));
            if (id > itr->first) return Status::ok();
            auto s = action(itr->first, itr->second);
            if (!s.is_ok()) return s;
            m_segments.erase(itr);
        }
        return Status::ok();
    }

    [[nodiscard]]
    auto most_recent_id() const -> SegmentId
    {
        if (m_segments.empty())
            return SegmentId::null();
        return crbegin(m_segments)->first;
    }

    [[nodiscard]]
    auto map() const -> const std::map<SegmentId, WalMetadata>&
    {
        return m_segments;
    }

private:
    std::map<SegmentId, WalMetadata> m_segments;
    std::vector<RecordPosition> m_uncommitted;

    SegmentId m_id;
    WalMetadata m_meta;
};

class SegmentGuard {
public:
    SegmentGuard(WalCollection *source, SegmentId id, SequenceId lsn)
        : m_source {source}
    {
        m_source->start_segment(id, lsn);
    }

    ~SegmentGuard()
    {
        if (m_source.is_valid())
            m_source->abort_segment();
    }

    auto finish(bool has_commit) -> void
    {
        CALICO_EXPECT_TRUE((m_source.is_valid()));
        m_source->finish_segment(has_commit);
        m_source.reset();
    }

private:
    UniqueNullable<WalCollection*> m_source;
};

class LogHelper {
public:
    using Action = std::function<Status()>;

    LogHelper(Size block_size, Action action)
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

    auto advance_cursor(Size record_size) -> void
    {
        CALICO_EXPECT_LE(record_size, m_block.size() - m_position.offset.value);
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

protected:
    auto reset() -> void
    {
        m_position = LogPosition {};
    }

    std::string m_block;
    Action m_action;
    LogPosition m_position;
};

class WalFilter {
public:
    using Predicate = std::function<bool(WalPayloadType)>;

    explicit WalFilter(Predicate predicate)
        : m_predicate {std::move(predicate)}
    {}

    [[nodiscard]]
    auto should_admit(BytesView payload) const -> bool
    {
        return m_predicate(WalPayloadType {payload[0]});
    }

private:
    Predicate m_predicate;
};

[[nodiscard]]
inline auto read_exact_or_hit_eof(RandomReader &file, Bytes out, Size n) -> Status
{
    auto temp = out;
    auto s = file.read(temp, n * temp.size());
    if (!s.is_ok()) return s;

    if (temp.is_empty()) {
        return Status::logic_error("EOF");
    } else if (temp.size() != out.size()) {
        ThreePartMessage message;
        message.set_primary("could not read from log");
        message.set_detail("incomplete read");
        return message.system_error();
    }
    return s;
}

class SequentialLogReader final: public LogHelper {
public:
    explicit SequentialLogReader(Size block_size)
        : LogHelper {block_size, [this] {
              const auto offset = position().number.value + 1;
              return read_exact_or_hit_eof(*m_file, block(), offset);
          }}
    {}

    ~SequentialLogReader() override = default;

    [[nodiscard]]
    auto is_attached() const -> bool
    {
        return m_file != nullptr;
    }

    [[nodiscard]]
    auto reset_position() -> Status
    {
        reset();
        return read_exact_or_hit_eof(*m_file, block(), 0);
    }

    [[nodiscard]]
    auto attach(RandomReader *file) -> Status
    {
        CALICO_EXPECT_NE(file, nullptr);
        m_file.reset(file);
        return reset_position();
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

    auto present(LogPosition position, Bytes &out) -> Status
    {
        auto tail = stob(m_tail);
        if (!m_has_block || position.number != m_block_num) {
            auto s = read_exact_or_hit_eof(*m_file, tail, position.number.value);
            if (!s.is_ok()) {
                // Keep the last block if we hit EOF. No sense in throwing it away I suppose.
                if (!s.is_logic_error())
                    m_has_block = false;
                return s;
            }
            m_block_num = position.number;
            m_has_block = true;
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
    explicit AppendLogWriter(Size block_size, Size existing_block_count = 0)
        : LogHelper {block_size, [this] {
              // Clear unused bytes at the end of the tail buffer.
              mem_clear(remaining());
              auto s = m_file->write(block());
              m_block_count += s.is_ok();
              return s;
          }},
          m_block_count {existing_block_count}
    {
        m_position.number.value = existing_block_count;
    }

    ~AppendLogWriter() override = default;

    [[nodiscard]]
    auto block_count() const -> Size
    {
        return m_block_count;
    }

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
    auto detach() -> Status
    {
        auto s = Status::ok();
        if (position().offset.value)
            s = advance_block();
        if (s.is_ok())
            s = m_file->sync();
        m_file.reset();
        m_block_count = 0;
        reset();
        return s;
    }

private:
    std::unique_ptr<AppendWriter> m_file;
    Size m_block_count {};
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
    auto get() -> NamedScratch
    {
        std::lock_guard lock {m_mutex};
        return m_manager.get();
    }

    auto put(NamedScratch scratch) -> void
    {
        std::lock_guard lock {m_mutex};
        m_manager.put(scratch);
    }

private:
    mutable std::mutex m_mutex;
    NamedScratchManager m_manager;
};

} // namespace calico

#endif // CALICO_WAL_HELPERS_H
