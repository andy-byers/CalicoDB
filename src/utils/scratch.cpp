#include "scratch.h"

namespace cco::utils {

auto ScratchManager::get() -> Scratch
{
    if (m_available.empty()) {
        m_occupied.emplace_back(m_scratch_size, '\x00');
    } else {
        m_occupied.emplace_back(std::move(m_available.back()));
        m_available.pop_back();
    }
    return Scratch {stob(m_occupied.back())};
}

auto ScratchManager::reset() -> void
{
    m_available.splice(end(m_available), m_occupied);
}

} // calico::utils