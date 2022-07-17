
#include "page_cache.h"
#include "frame.h"

namespace cco {

auto PageCache::put(PID id, Frame frame) -> void
{
    CCO_EXPECT_FALSE(m_cold.contains(id));
    CCO_EXPECT_FALSE(m_hot.contains(id));
    m_cold.put(id, std::move(frame));
}

auto PageCache::get(PID id) -> std::optional<Reference>
{
    if (auto ref = m_hot.get(id)) {
        m_hits++;
        return ref;
    }
    if (auto ref = m_cold.extract(id)) {
        m_hits++;
        m_hot.put(id, std::move(*ref));
        return m_hot.get(id);
    }
    m_misses++;
    return std::nullopt;
}

auto PageCache::extract(PID id) -> std::optional<Frame>
{
    if (auto frame = m_hot.extract(id)) {
        m_hits++;
        return frame;
    }
    if (auto frame = m_cold.extract(id)) {
        m_hits++;
        return frame;
    }
    m_misses++;
    return std::nullopt;
}

auto PageCache::evict() -> std::optional<Frame>
{
    if (auto frame = m_hot.evict())
        return frame;
    return m_cold.evict();
}

} // cco