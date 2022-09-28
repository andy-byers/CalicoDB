#include "cleaner.h"
#include "reader.h"

namespace calico {

auto WalCleaner::on_event(const SequenceId &limit) -> Status
{
    auto first = m_set->first();
    auto current = first;
    SegmentId target;

    while (!current.is_null()) {
        SequenceId first_lsn;
        auto s = read_first_lsn(
            *m_store, m_prefix, current, first_lsn);

        if (s.is_ok()) {
            if (first_lsn >= limit)
                break;
        } else if (!s.is_not_found()) {
            return s;
        }
        if (!target.is_null()) {
            CALICO_TRY(m_store->remove_file(m_prefix + target.to_name()));
            m_set->remove_before(current);
        }

        target = std::exchange(current, m_set->id_after(current));
    }
    return Status::ok();
}

} // namespace calico