#include "scratch.h"
#include "expect.h"

namespace calico {

auto NamedScratchManager::get() -> NamedScratch
{
    std::string scratch;
    if (m_available.empty()) {
        scratch.resize(m_scratch_size);
    } else {
        scratch = std::move(m_available.back());
        m_available.pop_back();
    }
    const auto id = m_next_id++;
    auto [itr, truthy] = m_occupied.emplace(id, std::move(scratch));
    CALICO_EXPECT_TRUE(truthy);

    return NamedScratch {id, stob(itr->second)};
}

auto NamedScratchManager::put(NamedScratch scratch) -> void
{
    auto itr = m_occupied.find(scratch.id());
    CALICO_EXPECT_NE(itr, end(m_occupied));
    m_available.emplace_back(std::move(itr->second));
    m_occupied.erase(itr);
}

} // namespace calico