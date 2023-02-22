#include "cleanup.h"
#include <atomic>

namespace Calico {

auto WalCleanup::cleanup(Lsn limit) -> void
{
    for (; ; ) {
        const auto id = m_set->first();
        if (id.is_null()) {
            return;
        }
        const auto next_id = m_set->id_after(id);
        if (next_id.is_null()) {
            return;
        }

        Lsn lsn;
        auto s = read_first_lsn(*m_storage, m_prefix, next_id, *m_set, lsn);
        if (!s.is_ok() && !s.is_not_found()) {
            m_error->set(s);
            return;
        }

        if (lsn > limit) {
            return;
        }
        s = m_storage->remove_file(encode_segment_name(m_prefix, id));
        if (s.is_ok()) {
            m_set->remove_before(next_id);
        } else {
            m_error->set(std::move(s));
            return;
        }
    }
}

} // namespace Calico