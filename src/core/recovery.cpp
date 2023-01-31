#include "recovery.h"
#include "pager/page.h"
#include "pager/pager.h"
#include "wal/wal.h"

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

#define ENSURE_NO_XACT(primary) \
    do { \
        if (system->has_xact) { \
            return logic_error( \
                "{}: a transaction is active", primary); \
        } \
    } while (0)

auto Recovery::start_abort() -> Status
{
    ENSURE_NO_XACT("cannot start abort");
    m_log->info("rolling back from lsn {}", m_wal->current_lsn().value);

    // This should give us the full images of each updated page belonging to the current transaction,
    // before any changes were made to it.
    return m_wal->roll_backward(system->commit_lsn, [this](auto payload) {
        auto decoded = decode_payload(payload);

        if (std::holds_alternative<std::monostate>(decoded)) {
            return corruption("WAL is corrupted");
        }

        if (std::holds_alternative<FullImageDescriptor>(decoded)) {
            const auto image = std::get<FullImageDescriptor>(decoded);
            CALICO_TRY_S(with_page(*m_pager, image, [this, &image](auto &page) {
                m_pager->upgrade(page);
                apply_undo(page, image);
            }));
        }
        return ok();
    });
}

auto Recovery::finish_abort() -> Status
{
    ENSURE_NO_XACT("cannot finish abort");
    CALICO_TRY_S(m_pager->flush({}));
    CALICO_TRY_S(m_wal->truncate(system->commit_lsn));
    m_log->info("rolled back to lsn {}", system->commit_lsn.value);
    return ok();
}

auto Recovery::start_recovery() -> Status
{
    ENSURE_NO_XACT("cannot start recovery");
    Lsn last_lsn;

    const auto redo = [this, &last_lsn](auto payload) {
        auto decoded = decode_payload(payload);

        // Payload has an invalid type.
        if (std::holds_alternative<std::monostate>(decoded)) {
            return corruption("WAL is corrupted");
        }

        last_lsn = payload.lsn();

        if (std::holds_alternative<CommitDescriptor>(decoded)) {
            system->commit_lsn = payload.lsn();
        } else if (std::holds_alternative<DeltaDescriptor>(decoded)) {
            const auto delta = std::get<DeltaDescriptor>(decoded);
            CALICO_TRY_S(with_page(*m_pager, delta, [this, &delta](auto &page) {
                if (delta.lsn > read_page_lsn(page)) {
                    m_pager->upgrade(page);
                    apply_redo(page, delta);
                }
            }));
        } else if (std::holds_alternative<FullImageDescriptor>(decoded)) {
            // This is not necessary in most cases, but should help with some kinds of corruption.
            const auto image = std::get<FullImageDescriptor>(decoded);
            CALICO_TRY_S(with_page(*m_pager, image, [this, &image](auto &page) {
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
            CALICO_TRY_S(with_page(*m_pager, image, [this, &image](auto &page) {
                m_pager->upgrade(page);
                apply_undo(page, image);
            }));
        }
        return ok();
    };
    m_log->info("rolling forward from lsn {}", m_pager->recovery_lsn().value);

    // Apply updates that are in the WAL but not the database.
    CALICO_TRY_S(m_wal->roll_forward(m_pager->recovery_lsn(), redo));
    m_log->info("rolled forward to lsn {}", last_lsn.value);

    // Reached the end of the WAL, but didn't find a commit record. Undo updates until we reach the most-recent commit.
    if (last_lsn != system->commit_lsn) {
        m_log->warn("missing commit record: rolling backward");
        CALICO_TRY_S(m_wal->roll_backward(system->commit_lsn, undo));
        m_log->info("rolled backward to lsn {}", system->commit_lsn.value);
    }
    return ok();
}

auto Recovery::finish_recovery() -> Status
{
    ENSURE_NO_XACT("cannot finish recovery");
    CALICO_TRY_S(m_pager->flush({}));
    m_wal->cleanup(m_pager->recovery_lsn());
    return ok();
}

#undef ENSURE_NO_XACT

} // namespace Calico