/*
 * Very loosely based off of https://medium.com/@koushikmohan/an-analysis-of-2q-cache-replacement-algorithms-21acceae672a
 */

#ifndef CCO_POOL_PAGE_CACHE_H
#define CCO_POOL_PAGE_CACHE_H

#include "frame.h"
#include "utils/cache.h"
#include "utils/identifier.h"
#include <list>
#include <unordered_map>

namespace cco {

class Frame;

class DirtyList final {
public:
    using Iterator = std::list<PageId>::iterator;

    DirtyList() = default;
    ~DirtyList() = default;

    [[nodiscard]]
    auto add(const PageId &id) -> Iterator
    {
        return m_list.emplace(cend(m_list), id);
    }

    auto remove(Iterator itr) -> void
    {
        m_list.erase(itr);
    }

private:
    std::list<PageId> m_list;
};

struct CacheEntry {
    Pin pin;
    DirtyList::Iterator dirty_itr {};
};

class PageCache final {
public:
    using WarmCache = UniqueFifoCache<PageId, CacheEntry, PageId::Hash>;
    using HotCache = UniqueLruCache<PageId, CacheEntry, PageId::Hash>;

    using Iterator = HotCache::Iterator;
    using ConstIterator = HotCache::ConstIterator;

    PageCache() = default;
    ~PageCache() = default;

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

    auto put(CacheEntry) -> void;
    auto get(PageId) -> Iterator;
    auto extract(PageId) -> std::optional<CacheEntry>;
    auto evict() -> std::optional<CacheEntry>;

private:
    WarmCache m_warm;
    HotCache m_hot;
    Size m_hits {};
    Size m_misses {};
};

} // namespace cco

#endif // CCO_POOL_PAGE_CACHE_H
