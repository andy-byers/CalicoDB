#include "cleaner.h"

namespace Calico {

auto WalCleaner::on_event(const Id &limit) -> Status
{
    auto first = m_set->first();
    auto current = first;
    SegmentId target;

    while (!current.is_null() && m_set->segments().size() > 1) { // TODO: Changed this to make sure we never remove the last segment.
        Id first_lsn;
        auto s = read_first_lsn(
            *m_store, m_prefix, current, first_lsn);

        if (s.is_ok()) {
            if (first_lsn >= limit)
                break;
        } else if (!s.is_not_found()) {
            return s;
        }
        if (!target.is_null()) {
            CALICO_TRY_S(m_store->remove_file(m_prefix + target.to_name()));
            m_set->remove_before(current);
        }

        target = std::exchange(current, m_set->id_after(current));
    }
    return ok();
}

} // namespace Calico