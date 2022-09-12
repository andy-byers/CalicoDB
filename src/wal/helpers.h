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
    auto most_recent_id() const -> SegmentId
    {
        std::lock_guard lock {m_mutex};
        if (m_segments.empty())
            return SegmentId::null();
        return *crbegin(m_segments);
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
