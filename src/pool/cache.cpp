
#include "cache.h"
#include "frame.h"

namespace calico {

auto PageCache::put(PID id, Frame frame) -> std::optional<Frame>
{
    m_entry.put(id, std::move(frame));
}

auto PageCache::get(PID id) -> std::optional<Reference>
{

}

auto PageCache::get(PID id) const -> std::optional<ConstReference>
{

}

auto PageCache::extract(PID id) -> std::optional<Frame>
{
    if (auto frame = m_hot.extract(id))
        return frame;
    return m_hot.extract(id);
}

auto PageCache::evict() -> std::optional<Frame>
{
    if (auto frame = m_entry.evict())
        return frame;
    return m_hot.evict();
}

} // calico