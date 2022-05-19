
#include "frame.h"
#include "cache.h"
#include "utils/utils.h"

namespace cub {

auto PageCache::is_empty() const -> Size 
{
    return size() == 0;
}

auto PageCache::size() const -> Size 
{
    return m_list.size();
}

auto PageCache::contains(PID id) const -> bool 
{
    return m_map.find(id) != m_map.end();
}

auto PageCache::hit_ratio() const -> double
{
    if (const auto total = static_cast<double>(m_hit_count + m_miss_count); total != 0.0)
        return static_cast<double>(m_hit_count) / total;
    return 0.0;
}

auto PageCache::put(Frame frame) -> void
{
    const auto page_id = frame.page_id();
    CUB_EXPECT_FALSE(contains(page_id));
    m_list.push_back(std::move(frame));
    m_map.emplace(page_id, std::prev(m_list.end()));
}

auto PageCache::evict(LSN flushed_lsn) -> std::optional<Frame>
{
    const auto can_evict = [flushed_lsn](const Frame &frame) {
        return !frame.is_dirty() || frame.page_lsn().value <= flushed_lsn.value;
    };
    for (auto itr = m_list.begin(); itr != m_list.end(); ++itr) {
        if (auto &frame = *itr; can_evict(frame)) {
            auto taken = std::move(frame);
            m_map.erase(taken.page_id());
            m_list.erase(itr);
            return taken;
        }
    }
    return std::nullopt;
}

/**
 * Effectively clear out the cache.
 *
 * Guarantees that all frames are clean and nothing can be located in the cache. Available
 * frames must be obtained by calling evict(). The caller must ensure that no pages are lent
 * out when this method is called.
 */
auto PageCache::purge() -> void
{
//    CUB_EXPECT_EQ(size(), m_frame_count);
//    for (auto &page: m_list)
//        frame->reset(PID{});
//    m_map.clear();
}

auto PageCache::extract(PID id) -> std::optional<Frame>
{
    if (auto itr = m_map.find(id); itr != m_map.end()) {
        auto frame = std::move(*itr->second);
        m_list.erase(itr->second);
        m_map.erase(itr);
        m_hit_count++;
        return frame;
    }
    m_miss_count++;
    return std::nullopt;
}

} // cub