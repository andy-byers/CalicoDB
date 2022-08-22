#ifndef CALICO_WAL_HELPERS_H
#define CALICO_WAL_HELPERS_H

#include <mutex>
#include <set>
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
    auto block() const -> BytesView
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

struct SegmentInfo {
    auto operator<(const SegmentInfo &rhs) const -> bool
    {
        return id < rhs.id;
    }

    SegmentId id;
    bool has_commit {};
};

class WalCollection final {
public:
    WalCollection() = default;
    ~WalCollection() = default;

    [[nodiscard]]
    auto is_segment_started() const -> bool
    {
        return !m_temp.id.is_null();
    }

    auto start_segment(SegmentId id) -> void
    {
        CALICO_EXPECT_FALSE(is_segment_started());
        m_temp.id = id;
        m_temp.has_commit = false;
    }

    auto abort_segment() -> void
    {
        CALICO_EXPECT_TRUE(is_segment_started());
        m_temp = {};
    }

    auto finish_segment(bool has_commit) -> void
    {
        CALICO_EXPECT_TRUE(is_segment_started());
        m_temp.has_commit = has_commit;
        m_info.emplace(m_temp);
        abort_segment();
    }

    [[nodiscard]]
    auto current_segment() const -> const SegmentInfo&
    {
        CALICO_EXPECT_TRUE(is_segment_started());
        return m_temp;
    }

    template<class Action>
    [[nodiscard]]
    auto remove_segments_from_left(SegmentId id, const Action &action) -> Status
    {
        // Removes segments from [<begin>, id).
        for (auto itr = cbegin(m_info); itr != cend(m_info); ) {
            if (id == itr->id) return Status::ok();
            auto s = action(*itr);
            if (!s.is_ok()) return s;
            itr = m_info.erase(itr);
        }
        return Status::ok();
    }

    template<class Action>
    [[nodiscard]]
    auto remove_segments_from_right(SegmentId id, const Action &action) -> Status
    {
        // Removes segments from [id, <end>) in reverse.
        while (!m_info.empty()) {
            const auto itr = prev(cend(m_info));
            if (id > itr->id) return Status::ok();
            auto s = action(*itr);
            if (!s.is_ok()) return s;
            m_info.erase(itr);
        }
        return Status::ok();
    }

    [[nodiscard]]
    auto most_recent_id() const -> SegmentId
    {
        if (m_info.empty())
            return SegmentId::null();
        return crbegin(m_info)->id;
    }

    [[nodiscard]]
    auto set() const -> const std::set<SegmentInfo>&
    {
        return m_info;
    }

private:
    std::set<SegmentInfo> m_info;
    SegmentInfo m_temp;
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

class SequentialLogReader final {
public:
    explicit SequentialLogReader(Size block_size)
        : m_buffer {block_size}
    {}

    ~SequentialLogReader() = default;

    [[nodiscard]]
    auto is_attached() const -> bool
    {
        return m_file != nullptr;
    }

    [[nodiscard]]
    auto reset_position() -> Status
    {
        m_buffer.reset();
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

    [[nodiscard]]
    auto position() const -> LogPosition
    {
        LogPosition position;
        position.number.value = m_buffer.block_number();
        position.offset.value = m_buffer.block_offset();
        return position;
    }

    [[nodiscard]]
    auto remaining() -> Bytes
    {
        return m_buffer.remaining();
    }

    [[nodiscard]]
    auto block() -> Bytes
    {
        return m_buffer.block();
    }

    [[nodiscard]]
    auto block() const -> BytesView
    {
        return m_buffer.block();
    }

    auto advance_cursor(Size record_size) -> void
    {
        m_buffer.advance_cursor(record_size);
    }

    [[nodiscard]]
    auto advance_block() -> Status
    {
        return m_buffer.advance_block([this] {
            const auto offset = m_buffer.block_number() + 1;
            return read_exact_or_hit_eof(*m_file, block(), offset);
        });
    }

private:
    std::unique_ptr<RandomReader> m_file;
    WalBuffer m_buffer;
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
