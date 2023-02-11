#include "cleanup.h"
#include <atomic>

namespace Calico {

auto WalCleanup::cleanup() -> void
{
    const auto limit = m_limit->load();

    for (; ; ) {
        const auto id = m_set->first();
        if (id.is_null()) {
            return;
        }
        const auto next_id = m_set->id_after(id);
        if (next_id.is_null()) {
            return;
        }

        auto lsn = read_first_lsn(*m_storage, m_prefix, next_id, *m_set);
        if (!lsn.has_value()) {
            m_error->set(std::move(lsn.error()));
            return;
        }

        if (*lsn > limit) {
            return;
        }
        auto s = m_storage->remove_file(encode_segment_name(m_prefix, id));
        if (s.is_ok()) {
            m_set->remove_before(next_id);
        } else {
            m_error->set(std::move(s));
            return;
        }
    }
}

} // namespace Calico