#include "scratch.h"
#include "expect.h"

namespace cco {

auto ScratchManager::get() -> Scratch
{
    if (m_counter >= MAXIMUM_SCRATCHES - 1) {
        CCO_EXPECT_EQ(m_counter, MAXIMUM_SCRATCHES - 1);
        m_emergency.emplace_back(m_scratch_size, '\x00');
        return Scratch {stob(m_emergency.back())};
    }
    return Scratch {stob(m_scratches[m_counter++])};
}

auto ScratchManager::reset() -> void
{
    m_counter = 0;
    m_emergency.clear();
}

auto ManualScratchManager::get() -> ManualScratch
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
    CCO_EXPECT_TRUE(truthy);
    return ManualScratch {id, stob(itr->second)};
}

auto ManualScratchManager::put(ManualScratch scratch) -> void
{
    auto itr = m_occupied.find(scratch.id());
    CCO_EXPECT_NE(itr, end(m_occupied));
    m_available.emplace_back(std::move(itr->second));
    m_occupied.erase(itr);
}

} // namespace cco