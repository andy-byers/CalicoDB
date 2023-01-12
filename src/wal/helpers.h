#ifndef CALICO_WAL_HELPERS_H
#define CALICO_WAL_HELPERS_H

#include <mutex>
#include <map>
#include <tl/expected.hpp>
#include "record.h"
#include "calico/storage.h"
#include "page/delta.h"
#include "utils/queue.h"
#include "utils/scratch.h"
#include "utils/system.h"
#include "utils/types.h"

namespace Calico {

static constexpr Size DELTA_PAYLOAD_HEADER_SIZE {11};
static constexpr Size IMAGE_PAYLOAD_HEADER_SIZE {9};
static constexpr Size COMMIT_PAYLOAD_HEADER_SIZE {1};

[[nodiscard]]
inline constexpr auto wal_block_size(Size page_size) -> Size
{
    return std::min(MAXIMUM_PAGE_SIZE, page_size * WAL_BLOCK_SCALE);
}

[[nodiscard]]
inline constexpr auto wal_scratch_size(Size page_size) -> Size
{
    return page_size + DELTA_PAYLOAD_HEADER_SIZE + sizeof(PageDelta);
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
        m_segments.emplace(id, Id::null());
    }

    auto add_segment(SegmentId id, Id first_lsn) -> void
    {
        std::lock_guard lock {m_mutex};
        m_segments.emplace(id, first_lsn);
    }

    [[nodiscard]]
    auto first_lsn(SegmentId id) const -> Id
    {
        std::lock_guard lock {m_mutex};

        const auto itr = m_segments.find(id);
        if (itr == end(m_segments))
            return Id::null();

        return itr->second;
    }

    auto set_first_lsn(SegmentId id, Id lsn) -> void
    {
        std::lock_guard lock {m_mutex};

        auto itr = m_segments.find(id);
        CALICO_EXPECT_NE(itr, end(m_segments));
        itr->second = lsn;
    }

    auto first() const -> SegmentId
    {
        std::lock_guard lock {m_mutex};
        return m_segments.empty() ? SegmentId::null() : cbegin(m_segments)->first;
    }

    auto last() const -> SegmentId
    {
        std::lock_guard lock {m_mutex};
        return m_segments.empty() ? SegmentId::null() : crbegin(m_segments)->first;
    }

    auto id_before(SegmentId id) const -> SegmentId
    {
        std::lock_guard lock {m_mutex};
        if (m_segments.empty())
            return SegmentId::null();

        auto itr = m_segments.lower_bound(id);
        if (itr == cbegin(m_segments))
            return SegmentId::null();
        return prev(itr)->first;
    }

    auto id_after(SegmentId id) const -> SegmentId
    {
        std::lock_guard lock {m_mutex};
        auto itr = m_segments.upper_bound(id);
        return itr != cend(m_segments) ? itr->first : SegmentId::null();
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
    auto segments() const -> const std::map<SegmentId, Id> &
    {
        // WARNING: We must ensure that background threads that modify the collection are paused before using this method.
        return m_segments;
    }

private:
    mutable std::mutex m_mutex;
    std::map<SegmentId, Id> m_segments;
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
        return m_manager.get();
    }

private:
    // Number of extra scratch buffers to allocate. We seem to need a number of buffers equal to the worker
    // queue size N + 2. This allows N buffers to be waiting in the queue, 1 for the WAL writer to work on,
    // and another for the pager to work on. Then we won't overwrite scratch memory that is in use, because
    // the worker queue will block after it reaches N elements.
    static constexpr Size EXTRA_SIZE {2};

    MonotonicScratchManager m_manager;
};


[[nodiscard]]
inline auto read_first_lsn(Storage &store, const std::string &prefix, SegmentId id, WalSet &set) -> tl::expected<Id, Status>
{
    if (auto lsn = set.first_lsn(id); !lsn.is_null())
        return lsn;

    RandomReader *temp;
    auto s = store.open_random_reader(prefix + id.to_name(), &temp);
    if (!s.is_ok())
        return tl::make_unexpected(s);

    char buffer[WalPayloadHeader::SIZE];
    Bytes bytes {buffer, sizeof(buffer)};
    std::unique_ptr<RandomReader> file {temp};

    // Read the first LSN. If it exists, it will always be at the same location.
    auto read_size = bytes.size();
    s = file->read(bytes.data(), read_size, WalRecordHeader::SIZE);
    if (!s.is_ok())
        return tl::make_unexpected(s);

    bytes.truncate(read_size);

    if (bytes.is_empty())
        return tl::make_unexpected(not_found("segment is empty"));

    if (bytes.size() != WalPayloadHeader::SIZE)
        return tl::make_unexpected(corruption("incomplete record"));

    auto lsn = read_wal_payload_header(bytes).lsn;
    set.set_first_lsn(id, lsn);
    return lsn;
}


} // namespace Calico

#endif // CALICO_WAL_HELPERS_H
