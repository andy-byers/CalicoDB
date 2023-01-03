
#include "page_cache.h"

namespace Calico {

auto PageCache::put(Id pid, Entry entry) -> void
{
    CALICO_EXPECT_FALSE(m_cache.contains(pid));
    m_cache.put(pid, entry);
}

auto PageCache::get(Id pid) -> Iterator
{
    using std::end;
    if (auto itr = m_cache.get(pid); itr != end(m_cache)) {
        m_hits++;
        return itr;
    }
    m_misses++;
    return end(*this);
}

auto PageCache::erase(Id pid) -> void
{
    CALICO_EXPECT_TRUE(m_cache.contains(pid));
    m_cache.erase(pid);
}

} // namespace Calico