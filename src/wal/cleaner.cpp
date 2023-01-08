#include "cleaner.h"

namespace Calico {

auto WalCleanupTask::operator()() -> void
{
    const auto limit = m_work.try_dequeue();

    if (!limit.has_value() || m_system->has_error())
        return;

    const auto first = m_set->first();
    if (first.is_null() || first <= *limit)
        return;

    const auto second = m_set->id_after(first);
    if (second.is_null() || second < *limit)
        return;

    auto s = m_storage->remove_file(m_prefix + first.to_name());
    if (s.is_ok()) {
        m_set->remove_before(second);
    } else {
        CALICO_ERROR(s);
    }
}

} // namespace Calico