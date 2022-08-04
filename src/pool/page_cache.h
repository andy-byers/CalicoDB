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

class PageCache final {
public:
    using Reference = std::reference_wrapper<Frame>;
    using ConstReference = std::reference_wrapper<const Frame>;
    using ColdCache = FifoCache<PageId, Frame, PageId::Hash>;
    using HotCache = LruCache<PageId, Frame, PageId::Hash>;

    PageCache() = default;
    ~PageCache() = default;

    [[nodiscard]] auto is_empty() const -> Size
    {
        return m_warm.is_empty() && m_hot.is_empty();
    }

    [[nodiscard]] auto size() const -> Size
    {
        return m_warm.size() + m_hot.size();
    }

    [[nodiscard]] auto contains(PageId id) const -> bool
    {
        return m_warm.contains(id) || m_hot.contains(id);
    }

    [[nodiscard]] auto hit_ratio() const -> double
    {
        if (const auto total = static_cast<double>(m_hits + m_misses); total != 0.0)
            return static_cast<double>(m_hits) / total;
        return 0.0;
    }

    [[nodiscard]] auto cold_begin() -> ColdCache::Iterator
    {
        return m_warm.begin();
    }

    [[nodiscard]] auto cold_end() -> ColdCache::Iterator
    {
        return m_warm.end();
    }

    [[nodiscard]] auto hot_begin() -> ColdCache::Iterator
    {
        return m_hot.begin();
    }

    [[nodiscard]] auto hot_end() -> ColdCache::Iterator
    {
        return m_hot.end();
    }

    auto put(PageId, Frame) -> void;
    auto get(PageId) -> std::optional<Reference>;
    auto extract(PageId) -> std::optional<Frame>;
    auto evict() -> std::optional<Frame>;

private:
    FifoCache<PageId, Frame, PageId::Hash> m_warm;
    LruCache<PageId, Frame, PageId::Hash> m_hot;
    Size m_hits {};
    Size m_misses {};
};

} // namespace cco

#endif // CCO_POOL_PAGE_CACHE_H
