#include "cleaner.h"

namespace Calico {

// 0 1 2
//   L

//   0 1 2
// L

auto WalCleanupTask::cleanup() -> void
{
    // TODO: Bummer, we will need to read the LSNs of the first few segments to know if we can remove the first one. We should
    //       cache this value so we only read each one once. I'll do it later, since I'm working on the writer right now!
//
//    const auto limit = m_limit->load();
//    const auto first = m_set->first();

//    auto s = m_storage->remove_file(m_prefix + first.to_name());
//    if (s.is_ok()) {
//        m_set->remove_before(second);
//    } else {
//        CALICO_ERROR(s);
//    }
}

} // namespace Calico