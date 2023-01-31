#include "cleanup.h"

namespace Calico {

auto WalCleanup::cleanup() -> void
{
    const auto limit = m_limit->load();

    const auto first = m_set->first();
    if (first.is_null()) {
        return;
    }

    const auto second = m_set->id_after(first);
    if (second.is_null()) {
        return;
    }

    auto first_lsn = read_first_lsn(*m_storage, m_prefix, second, *m_set);
    if (!first_lsn.has_value()) {
        CALICO_ERROR(first_lsn.error());
        return;
    }

    if (*first_lsn > limit) {
        return;
    }

    auto s = m_storage->remove_file(m_prefix + encode_segment_name(first));
    if (s.is_ok()) {
        m_set->remove_before(second);
    } else {
        CALICO_ERROR(s);
    }
}

} // namespace Calico