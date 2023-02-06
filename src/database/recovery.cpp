#include "recovery.h"
#include "pager/page.h"
#include "pager/pager.h"

namespace Calico {

static auto apply_undo(Page &page, const FullImageDescriptor &image)
{
    mem_copy(page.span(0, page.size()), image.image);
}

static auto apply_redo(Page &page, const DeltaDescriptor &deltas)
{
    for (auto [offset, data]: deltas.deltas) {
        mem_copy(page.span(offset, data.size()), data);
    }
}

template<class Descriptor, class Callback>
static auto with_page(Pager &pager, const Descriptor &descriptor, const Callback &callback)
{
    if (auto page = pager.acquire(descriptor.pid)) {
        callback(*page);
        pager.release(std::move(*page));
        return ok();
    } else {
        return page.error();
    }
}

auto Recovery::start_abort() -> Status
{
    Calico_Info("rolling back from lsn {}", m_wal->current_lsn().value);

    // This should give us the full images of each updated page belonging to the current transaction,
    // before any changes were made to it.
    return m_wal->roll_backward(*m_commit_lsn, [this](auto payload) {
        auto decoded = decode_payload(payload);

        if (std::holds_alternative<std::monostate>(decoded)) {
            return corruption("WAL is corrupted");
        }

        if (std::holds_alternative<FullImageDescriptor>(decoded)) {
            const auto image = std::get<FullImageDescriptor>(decoded);
            Calico_Try_S(with_page(*m_pager, image, [this, &image](auto &page) {
                m_pager->upgrade(page);
                apply_undo(page, image);
            }));
        }
        return ok();
    });
}

auto Recovery::finish_abort() -> Status
{
    Calico_Try_S(m_pager->flush({}));
    Calico_Try_S(m_wal->truncate(*m_commit_lsn));
    Calico_Info("rolled back to lsn {}", m_commit_lsn->value);

    if (m_pager->recovery_lsn() > *m_commit_lsn) {
        m_pager->set_recovery_lsn(*m_commit_lsn); // TODO
    }
    return ok();
}

auto Recovery::start_recovery() -> Status
{
    Lsn last_lsn;

    const auto redo = [this, &last_lsn](auto payload) {
        auto decoded = decode_payload(payload);

        // Payload has an invalid type.
        if (std::holds_alternative<std::monostate>(decoded)) {
            return corruption("WAL is corrupted");
        }

        last_lsn = payload.lsn();

        if (std::holds_alternative<CommitDescriptor>(decoded)) {
            *m_commit_lsn = payload.lsn();
        } else if (std::holds_alternative<DeltaDescriptor>(decoded)) {
            const auto delta = std::get<DeltaDescriptor>(decoded);
            Calico_Try_S(with_page(*m_pager, delta, [this, &delta](auto &page) {
                if (delta.lsn > read_page_lsn(page)) {
                    m_pager->upgrade(page);
                    apply_redo(page, delta);
                }
            }));
        } else if (std::holds_alternative<FullImageDescriptor>(decoded)) {
            // This is not necessary in most cases, but should help with some kinds of corruption.
            const auto image = std::get<FullImageDescriptor>(decoded);
            Calico_Try_S(with_page(*m_pager, image, [this, &image](auto &page) {
                if (image.lsn > read_page_lsn(page)) {
                    m_pager->upgrade(page);
                    apply_undo(page, image);
                }
            }));
        } else {
            return corruption("unrecognized payload type");
        }
        return ok();
    };

    const auto undo = [this](auto payload) {
        auto decoded = decode_payload(payload);

        if (std::holds_alternative<std::monostate>(decoded)) {
            return corruption("WAL is corrupted");
        }

        if (std::holds_alternative<FullImageDescriptor>(decoded)) {
            const auto image = std::get<FullImageDescriptor>(decoded);
            Calico_Try_S(with_page(*m_pager, image, [this, &image](auto &page) {
                m_pager->upgrade(page);
                apply_undo(page, image);
            }));
        }
        return ok();
    };
    Calico_Info("rolling forward from lsn {}", m_pager->recovery_lsn().value);

    // Apply updates that are in the WAL but not the database.
    Calico_Try_S(m_wal->roll_forward(m_pager->recovery_lsn(), redo));
    Calico_Info("rolled forward to lsn {}", last_lsn.value);

    // Reached the end of the WAL, but didn't find a commit record. Undo updates until we reach the most-recent commit.
    if (last_lsn != *m_commit_lsn) {
        Calico_Warn("missing commit record: rolling backward");
        Calico_Try_S(m_wal->roll_backward(*m_commit_lsn, undo));
        Calico_Info("rolled backward to lsn {}", m_commit_lsn->value);
    }
    return ok();
}

auto Recovery::finish_recovery() -> Status
{
    fprintf(stderr,"recovered: wal flushed lsn is %zu\n", m_wal->flushed_lsn().value);
    Calico_Try_S(m_pager->flush({}));
    m_wal->cleanup(m_pager->recovery_lsn());
    return ok();
}

#undef ENSURE_NO_XACT

} // namespace Calico