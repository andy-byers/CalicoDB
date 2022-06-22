#include "cache.h"
#include "frame.h"

namespace calico {

auto PageCache::put(Frame frame) -> void
{
    const auto page_id = frame.page_id();
    CALICO_EXPECT_FALSE(contains(page_id));
    m_dirty_count += frame.is_dirty();
    m_list.push_back(std::move(frame));
    m_map.emplace(page_id, std::prev(m_list.end()));
}

auto PageCache::evict(LSN flushed_lsn) -> std::optional<Frame>
{
    const auto can_evict = [flushed_lsn](const Frame &frame) {
        return !frame.is_dirty() || frame.page_lsn() <= flushed_lsn;
    };
    for (auto itr = m_list.begin(); itr != m_list.end(); ++itr) {
        if (auto &frame = *itr; can_evict(frame)) {
            m_dirty_count -= frame.is_dirty();
            auto taken = std::move(frame);
            m_map.erase(taken.page_id());
            m_list.erase(itr);
            return taken;
        }
    }
    return std::nullopt;
}

auto PageCache::extract(PID id) -> std::optional<Frame>
{
    if (auto itr = m_map.find(id); itr != m_map.end()) {
        auto frame = std::move(*itr->second);
        m_dirty_count -= frame.is_dirty();
        m_list.erase(itr->second);
        m_map.erase(itr);
        m_hit_count++;
        return frame;
    }
    m_miss_count++;
    return std::nullopt;
}

} // calico