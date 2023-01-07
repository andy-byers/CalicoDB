#ifndef CALICO_WAL_HELPERS_H
#define CALICO_WAL_HELPERS_H

#include "calico/storage.h"
#include "record.h"
#include "utils/queue.h"
#include "utils/scratch.h"
#include "utils/system.h"
#include "utils/types.h"
#include <mutex>
#include <set>
#include <tl/expected.hpp>

namespace Calico {

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
    RandomReader *temp;
    CALICO_TRY_S(store.open_random_reader(prefix + id.to_name(), &temp));

    char buffer[WalPayloadHeader::SIZE];
    Bytes bytes {buffer, sizeof(buffer)};
    std::unique_ptr<RandomReader> file {temp};

    // Read the first LSN. If it exists, it will always be at the same location.
    auto read_size = bytes.size();
    CALICO_TRY_S(file->read(bytes.data(), read_size, WalRecordHeader::SIZE));
    bytes.truncate(read_size);

    if (bytes.is_empty())
        return not_found("segment is empty");

    if (bytes.size() != WalPayloadHeader::SIZE)
        return corruption("incomplete record");

    out = read_wal_payload_header(bytes).lsn;
    return ok();
}

/*
 * Stores a collection of WAL segment descriptors and provides synchronized access.
 */
class WalSet final {
public:
    WalSet() = default;
    ~WalSet() = default;

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
        if (m_segments.empty())
            return SegmentId::null();

        auto itr = m_segments.lower_bound(id);
        if (itr == cbegin(m_segments))
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
    explicit LogScratchManager(Size buffer_size, Size buffer_count)
        : m_manager {buffer_size, buffer_count + EXTRA_SIZE}
    {}

    ~LogScratchManager() = default;

    [[nodiscard]]
    auto get() -> Scratch
    {
        std::lock_guard lock {m_mutex};
        return m_manager.get();
    }

private:
    // Number of extra scratch buffers to allocate. We seem to need a number of buffers equal to the worker
    // queue size N + 2. This allows N buffers to be waiting in the queue, 1 for the WAL writer to work on,
    // and another for the pager to work on. Then we won't overwrite scratch memory that is in use, because
    // the worker queue will block after it reaches N elements.
    static constexpr Size EXTRA_SIZE {2};
    mutable std::mutex m_mutex;
    MonotonicScratchManager m_manager;
};

} // namespace Calico

#endif // CALICO_WAL_HELPERS_H
