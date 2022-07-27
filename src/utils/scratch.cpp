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

} // cco