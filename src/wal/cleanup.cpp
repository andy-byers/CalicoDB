#include "cleanup.h"

namespace Calico {

auto WalCleanup::cleanup() -> void
{
    const auto limit = m_limit->load();

    const auto first = m_set->first();
    if (first.is_null())
        return;

    const auto second = m_set->id_after(first);
    if (second.is_null())
        return;

    Id lsn;
    CALICO_ERROR_IF(read_first_lsn(
        *m_storage, m_prefix, second, lsn));
    if (lsn > limit)
        return;

    auto s = m_storage->remove_file(m_prefix + first.to_name());
    if (s.is_ok()) {
        m_set->remove_before(second);
    } else {
        CALICO_ERROR(s);
    }
}

} // namespace Calico