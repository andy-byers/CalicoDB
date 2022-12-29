
#include "registry.h"

namespace calico {

auto PageRegistry::put(identifier pid, Size fid) -> void
{
    CALICO_EXPECT_FALSE(m_cache.contains(pid));
    m_cache.put(pid, Entry {fid});
}

auto PageRegistry::get(identifier id) -> Iterator
{
    using std::end;

    if (auto itr = m_cache.get(id); itr != m_cache.end()) {
        m_hits++;
        return itr;
    }
    m_misses++;
    return end(*this);
}

auto PageRegistry::erase(identifier id) -> void
{
    if (m_cache.erase(id))
        return;

    // Only erase pages we know are in the registry.
    CALICO_EXPECT_TRUE(false && "erase(): cannot find entry");
}

} // namespace calico