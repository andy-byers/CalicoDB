#ifndef CALICO_WAL_HELPERS_H
#define CALICO_WAL_HELPERS_H

#include "calico/storage.h"
#include "record.h"
#include "utils/logging.h"
#include "utils/queue.h"
#include "utils/result.h"
#include "utils/scratch.h"
#include "utils/types.h"
#include <mutex>
#include <set>

namespace calico {

class LogWriter_;

[[nodiscard]]
inline constexpr auto wal_block_size(Size page_size) -> Size
{
    return std::min(MAXIMUM_PAGE_SIZE, page_size * WAL_BLOCK_SCALE);
}

[[nodiscard]]
inline constexpr auto wal_scratch_size(Size page_size) -> Size
{
    return page_size * WAL_SCRATCH_SCALE;
}

class LogBuffer final {
public:
    explicit LogBuffer(Size size)
        : m_buffer(size, '\x00')
    {
        CALICO_EXPECT_TRUE(is_power_of_two(size));
    }

    ~LogBuffer() = default;

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
    auto offset() const -> Size
    {
        return m_number*m_buffer.size() + m_offset;
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

struct WalSegment {
    auto operator<(const WalSegment &rhs) const -> bool
    {
        return id < rhs.id;
    }

    SegmentId id;
    bool has_commit {};
};

/*
 * Stores a collection of WAL segment descriptors and provides synchronized access.
 */
class WalCollection final {
public:
    WalCollection() = default;
    ~WalCollection() = default;

    auto add_segment(WalSegment segment) -> void
    {
        std::lock_guard lock {m_mutex};
        m_segments.emplace(segment);
    }

    auto id_before(SegmentId id) const -> SegmentId
    {
        std::lock_guard lock {m_mutex};
        auto itr = m_segments.lower_bound({id});
        if (itr == cend(m_segments) || itr == cbegin(m_segments))
            return SegmentId::null();
        return prev(itr)->id;
    }

    auto id_after(SegmentId id) const -> SegmentId
    {
        std::lock_guard lock {m_mutex};
        auto itr = m_segments.upper_bound({id});
        return itr != cend(m_segments) ? itr->id : SegmentId::null();
    }

    auto remove_before(SegmentId id) -> void
    {
        // Removes segments in [<begin>, id).
        std::lock_guard lock {m_mutex};
        auto itr = m_segments.lower_bound({id});
        m_segments.erase(cbegin(m_segments), itr);
    }

    auto remove_after(SegmentId id) -> void
    {
        // Removes segments in (id, <end>).
        std::lock_guard lock {m_mutex};
        auto itr = m_segments.upper_bound({id});
        m_segments.erase(itr, cend(m_segments));
    }

    // TODO: Deprecated in favor of remove_before().
    template<class Action>
    [[nodiscard]]
    auto remove_from_left(SegmentId id, const Action &action) -> Status
    {
        std::lock_guard lock {m_mutex}; // TODO: We may see a bottleneck if we end up waiting for this a lot. I'm hoping unlinking files is much faster than writing...
        // Removes segments from [<begin>, id).
        for (auto itr = cbegin(m_segments); itr != cend(m_segments); ) {
            if (id == itr->id) return Status::ok();
            auto s = action(*itr);
            if (!s.is_ok()) return s;
            itr = m_segments.erase(itr);
        }
        return Status::ok();
    }

    // TODO: Deprecated in favor of remove_after().
    template<class Action>
    [[nodiscard]]
    auto remove_from_right(SegmentId id, const Action &action) -> Status
    {
        std::lock_guard lock {m_mutex};
        // Removes segments from [id, <end>) in reverse.
        while (!m_segments.empty()) {
            const auto itr = prev(cend(m_segments));
            if (id > itr->id) return Status::ok();
            auto s = action(*itr);
            if (!s.is_ok()) return s;
            m_segments.erase(itr);
        }
        return Status::ok();
    }

    [[nodiscard]]
    auto most_recent_id() const -> SegmentId
    {
        std::lock_guard lock {m_mutex};
        if (m_segments.empty())
            return SegmentId::null();
        return crbegin(m_segments)->id;
    }

    [[nodiscard]]
    auto segments() const -> const std::set<WalSegment>&
    {
        // WARNING: We must ensure that background threads that modify the collection are paused before using this method.
        return m_segments;
    }

private:
    mutable std::mutex m_mutex;
    std::set<WalSegment> m_segments;
};

/*
 * A scope guard that keeps the set of in-memory WAL segment descriptors consistent with what is on disk.
 */
class SegmentGuard final {
public:
    SegmentGuard(Storage &store, LogWriter_ &writer, WalCollection &collection, std::atomic<SequenceId> &flushed_lsn, std::string prefix)
        : m_prefix {std::move(prefix)},
          m_store {&store},
          m_writer {&writer},
          m_collection {&collection},
          m_flushed_lsn {&flushed_lsn}
    {}

    ~SegmentGuard();

    // No moves or copies!
    SegmentGuard(const SegmentGuard&) = delete;

    auto operator=(const SegmentGuard&) -> SegmentGuard& = delete;
    SegmentGuard(SegmentGuard&&) = delete;
    auto operator=(SegmentGuard&&) -> SegmentGuard& = delete;

    [[nodiscard]] auto start() -> Status;
    [[nodiscard]] auto finish(bool has_commit) -> Status;
    [[nodiscard]] auto abort() -> Status;
    [[nodiscard]] auto is_started() const -> bool;

    [[nodiscard]] auto id() const -> SegmentId
    {
        return is_started() ? m_current.id : SegmentId::null();
    }

private:
    std::string m_prefix;
    WalSegment m_current;
    Storage *m_store {};
    LogWriter_ *m_writer {};
    WalCollection *m_collection {};
    std::atomic<SequenceId> *m_flushed_lsn {};
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

class LogReader_ final {
public:
    explicit LogReader_(Size block_size)
        : m_buffer {block_size}
    {}

    ~LogReader_() = default;

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
    LogBuffer m_buffer;
};

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
