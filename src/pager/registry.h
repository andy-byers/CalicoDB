/*
 * Very loosely based off of https://medium.com/@koushikmohan/an-analysis-of-2q-cache-replacement-algorithms-21acceae672a
 */

#ifndef CALICO_PAGER_PAGE_CACHE_H
#define CALICO_PAGER_PAGE_CACHE_H

#include "framer.h"
#include "utils/cache.h"
#include "utils/types.h"

namespace calico {

class Frame;

class PageList final {
public:
    using Iterator = std::list<PageId>::iterator;

    PageList() = default;
    ~PageList() = default;

    [[nodiscard]]
    auto begin() -> Iterator
    {
        using std::begin;
        return begin(m_list);
    }

    [[nodiscard]]
    auto end() -> Iterator
    {
        using std::end;
        return end(m_list);
    }

    [[nodiscard]]
    auto insert(const PageId &id) -> Iterator
    {
        return m_list.emplace(cend(m_list), id);
    }

    auto remove(Iterator itr) -> Iterator
    {
        return m_list.erase(itr);
    }

private:
    std::list<PageId> m_list;
};

class PageRegistry final {
public:
    using DirtyToken = std::optional<PageList::Iterator>;

    struct Entry {
        FrameNumber frame_id;
        DirtyToken dirty_token {};
    };

    using WarmCache = UniqueFifoCache<PageId, Entry, PageId::Hash>;
    using HotCache = UniqueLruCache<PageId, Entry, PageId::Hash>;
    using Iterator = HotCache::Iterator;

    PageRegistry() = default;
    ~PageRegistry() = default;

    [[nodiscard]]
    auto is_empty() const -> Size
    {
        return m_warm.is_empty() && m_hot.is_empty();
    }

    [[nodiscard]]
    auto size() const -> Size
    {
        return m_warm.size() + m_hot.size();
    }

    [[nodiscard]]
    auto contains(PageId id) const -> bool
    {
        return m_hot.contains(id) || m_warm.contains(id);
    }

    [[nodiscard]]
    auto hit_ratio() const -> double
    {
        if (const auto total = static_cast<double>(m_hits + m_misses); total != 0.0)
            return static_cast<double>(m_hits) / total;
        return 0.0;
    }

    [[nodiscard]]
    auto end() -> Iterator
    {
        return m_hot.end();
    }

    template<class Callback>
    auto find_entry(const Callback &callback) -> Iterator
    {
        // Search through the warm cache first. m_warm.begin() should give the last element placed into that cache, and
        // prev(m_warm.end()) the last.
        for (auto itr = m_warm.begin(); itr != m_warm.end(); ++itr) {
            auto [frame_id, dirty_itr] = itr->second;
            if (callback(itr->first, frame_id, dirty_itr)) return itr;
        }
        for (auto itr = m_hot.begin(); itr != m_hot.end(); ++itr) {
            auto [frame_id, dirty_itr] = itr->second;
            if (callback(itr->first, frame_id, dirty_itr)) return itr;
        }
        return end();
    }

    auto put(PageId, FrameNumber) -> void;
    auto get(PageId) -> Iterator;
    auto erase(PageId) -> void;

private:
    WarmCache m_warm;
    HotCache m_hot;
    Size m_hits {};
    Size m_misses {};
};

} // namespace calico

#endif // CALICO_PAGER_PAGE_CACHE_H
