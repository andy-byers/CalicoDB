#ifndef CALICO_WAL_HELPERS_H
#define CALICO_WAL_HELPERS_H

#include "calico/storage.h"
#include "record.h"
#include "utils/info_log.h"
#include "utils/queue.h"
#include <tl/expected.hpp>
#include "utils/scratch.h"
#include "utils/types.h"
#include <mutex>
#include <set>

namespace calico {

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

[[nodiscard]]
inline auto read_first_lsn(Storage &store, const std::string &prefix, SegmentId id, Id &out) -> Status
{
    RandomReader *temp {};
    CALICO_TRY(store.open_random_reader(prefix + id.to_name(), &temp));

    char buffer[WalPayloadHeader::SIZE];
    Bytes bytes {buffer, sizeof(buffer)};
    std::unique_ptr<RandomReader> file {temp};

    // Read the first LSN. If it exists, it will always be at the same location.
    CALICO_TRY(file->read(bytes, WalRecordHeader::SIZE));

    if (bytes.is_empty())
        return Status::not_found("segment is empty");

    if (bytes.size() != WalPayloadHeader::SIZE)
        return Status::corruption("incomplete record");

    out = read_wal_payload_header(bytes).lsn;
    return Status::ok();
}

/*
 * Stores a collection of WAL segment descriptors and provides synchronized access.
 */
class WalCollection final {
public:
    WalCollection() = default;
    ~WalCollection() = default;

    auto add_segment(SegmentId id) -> void
    {
        std::lock_guard lock {m_mutex};
        m_segments.emplace(id);
    }

    auto first() const -> SegmentId
    {
        std::lock_guard lock {m_mutex};
        return m_segments.empty() ? SegmentId::null() : *cbegin(m_segments);
    }

    auto last() const -> SegmentId
    {
        std::lock_guard lock {m_mutex};
        return m_segments.empty() ? SegmentId::null() : *crbegin(m_segments);
    }

    auto id_before(SegmentId id) const -> SegmentId
    {
        std::lock_guard lock {m_mutex};
        auto itr = m_segments.lower_bound(id);
        if (itr == cend(m_segments) || itr == cbegin(m_segments))
            return SegmentId::null();
        return *prev(itr);
    }

    auto id_after(SegmentId id) const -> SegmentId
    {
        std::lock_guard lock {m_mutex};
        auto itr = m_segments.upper_bound(id);
        return itr != cend(m_segments) ? *itr : SegmentId::null();
    }

    auto remove_before(SegmentId id) -> void
    {
        // Removes segments in [<begin>, id).
        std::lock_guard lock {m_mutex};
        auto itr = m_segments.lower_bound(id);
        m_segments.erase(cbegin(m_segments), itr);
    }

    auto remove_after(SegmentId id) -> void
    {
        // Removes segments in (id, <end>).
        std::lock_guard lock {m_mutex};
        auto itr = m_segments.upper_bound(id);
        m_segments.erase(itr, cend(m_segments));
    }

    [[nodiscard]]
    auto segments() const -> const std::set<SegmentId>&
    {
        // WARNING: We must ensure that background threads that modify the collection are paused before using this method.
        return m_segments;
    }

private:
    mutable std::mutex m_mutex;
    std::set<SegmentId> m_segments;
};

class LogScratchManager final {
public:
    explicit LogScratchManager(Size buffer_size)
        : m_manager {buffer_size}
    {}

    ~LogScratchManager() = default;

    [[nodiscard]]
    auto get() -> Scratch
    {
        std::lock_guard lock {m_mutex};
        return m_manager.get();
    }

private:
    // NOTE: I know this seems pretty sketchy... It should be okay, because each call to get() is followed by
    //       a call to WriteAheadLog::log(), which pushes its arguments into a WORKER_CAPACITY-length queue,
    //       only blocking if it becomes full.
    static constexpr Size SCRATCH_COUNT {WORKER_CAPACITY + 2};

    mutable std::mutex m_mutex;
    MonotonicScratchManager<SCRATCH_COUNT> m_manager;
};

} // namespace calico

#endif // CALICO_WAL_HELPERS_H
