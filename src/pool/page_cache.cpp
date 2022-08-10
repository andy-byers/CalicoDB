
#include "page_cache.h"
#include "frame.h"

namespace cco {

auto PageCache::put(CacheEntry entry) -> void
{
    CCO_EXPECT_FALSE(m_warm.contains(entry.pid));
    CCO_EXPECT_FALSE(m_hot.contains(entry.pid));
    m_warm.put(entry.pin.pid, std::move(entry));
}

auto PageCache::get(PageId id) -> Iterator
{
    if (auto itr = m_hot.get(id); itr != m_hot.end()) {
        m_hits++;
        return itr;
    }
    if (auto itr = m_warm.extract(id)) {
        m_hits++;
        m_hot.put(id, std::move(*itr));
        return m_hot.get(id);
    }
    m_misses++;
    return m_hot.end();
}

auto PageCache::extract(PageId id) -> std::optional<CacheEntry>
{
    if (auto e = m_hot.extract(id)) {
        m_hits++;
        return e;
    }
    if (auto e = m_warm.extract(id)) {
        m_hits++;
        return e;
    }
    m_misses++;
    return std::nullopt;
}

auto PageCache::evict() -> std::optional<CacheEntry>
{
    if (auto e = m_hot.evict())
        return e;
    return m_warm.evict();
}

} // namespace cco