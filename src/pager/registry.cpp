
#include "registry.h"

namespace calico {

auto PageRegistry::put(identifier pid, Entry entry) -> void
{
    CALICO_EXPECT_FALSE(m_cache.contains(pid));
    m_cache.put(pid, entry);
}

auto PageRegistry::get(identifier pid) -> Iterator
{
    using std::end;
    if (auto itr = m_cache.get(pid); itr != end(m_cache)) {
        m_hits++;
        return itr;
    }
    m_misses++;
    return end(*this);
}

auto PageRegistry::erase(identifier pid) -> void
{
    CALICO_EXPECT_TRUE(m_cache.contains(pid));
    m_cache.erase(pid);
}

} // namespace calico