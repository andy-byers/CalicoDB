#include "recovery.h"
#include "page/page.h"
#include "pager/pager.h"
#include "wal/wal.h"

namespace Calico {

#define ENSURE_NO_XACT(primary) \
    do { \
        if (m_system->has_xact) \
            return logic_error( \
                "{}: a transaction is active", primary); \
    } while (0)

auto Recovery::start_abort() -> Status
{
    // m_log->trace("start_abort");
    ENSURE_NO_XACT("cannot start abort");

    // This should give us the full images of each updated page belonging to the current transaction,
    // before any changes were made to it.
    return m_wal->roll_backward(m_system->commit_lsn.load(), [this](auto payload) {
        auto decoded = decode_payload(payload);

        if (std::holds_alternative<std::monostate>(decoded))
            return corruption("WAL is corrupted");

        if (std::holds_alternative<FullImageDescriptor>(decoded)) {
            const auto image = std::get<FullImageDescriptor>(decoded);
            auto page = m_pager->acquire(image.pid, true);
            if (!page.has_value()) return page.error();
            page->apply_update(image);
            return m_pager->release(std::move(*page));
        }
        return ok();
    });
}

auto Recovery::finish_abort() -> Status
{
    // m_log->trace("finish_abort");
    ENSURE_NO_XACT("cannot finish abort");
    CALICO_TRY_S(m_pager->flush({}));

    return m_wal->truncate(m_system->commit_lsn);
}

auto Recovery::start_recovery() -> Status
{
    // m_log->trace("start_recovery");
    ENSURE_NO_XACT("cannot start recovery");
    Id last_lsn;

    const auto redo = [this, &last_lsn](auto payload) {
        auto decoded = decode_payload(payload);

        // Payload has an invalid type.
        if (std::holds_alternative<std::monostate>(decoded))
            return corruption("WAL is corrupted");

        last_lsn = payload.lsn();

        if (std::holds_alternative<CommitDescriptor>(decoded)) {
            m_system->commit_lsn.store(payload.lsn());
        } else if (std::holds_alternative<DeltaDescriptor>(decoded)) {
            const auto delta = std::get<DeltaDescriptor>(decoded);
            auto page = m_pager->acquire(delta.pid, true);
            if (!page.has_value())
                return page.error();
            if (delta.lsn > page->lsn())
                page->apply_update(delta);
            return m_pager->release(std::move(*page));
        } else if (std::holds_alternative<FullImageDescriptor>(decoded)) {
            // This is not necessary in most cases, but should help with some kinds of corruption.
            const auto image = std::get<FullImageDescriptor>(decoded);
            auto page = m_pager->acquire(image.pid, true);
            if (!page.has_value())
                return page.error();
            if (image.lsn > page->lsn())
                page->apply_update(image);
            return m_pager->release(std::move(*page));
        } else {
            return corruption("unrecognized payload type");
        }
        return ok();
    };

    const auto undo = [this](auto payload) {
        auto decoded = decode_payload(payload);

        if (std::holds_alternative<std::monostate>(decoded))
            return corruption("WAL is corrupted");

        if (std::holds_alternative<FullImageDescriptor>(decoded)) {
            const auto image = std::get<FullImageDescriptor>(decoded);
            auto page = m_pager->acquire(image.pid, true);
            if (!page.has_value()) return page.error();
            page->apply_update(image);
            return m_pager->release(std::move(*page));
        }
        return ok();
    };


    // Apply updates that are in the WAL but not the database.
    auto s = m_wal->roll_forward(m_pager->recovery_lsn(), redo);

    // m_log->info("{} -> {}", m_pager->recovery_lsn().value, last_lsn.value);
    CALICO_TRY_S(s);

    // Reached the end of the WAL, but didn't find a commit record. Undo updates until we reach the most-recent commit.
    const auto commit_lsn = m_system->commit_lsn.load();
    if (last_lsn != commit_lsn)
        return m_wal->roll_backward(commit_lsn, undo);
    return ok();
}

auto Recovery::finish_recovery() -> Status
{
    // m_log->trace("finish_recovery");
    ENSURE_NO_XACT("cannot finish recovery");

    CALICO_TRY_S(m_pager->flush({}));
    m_wal->cleanup(m_pager->recovery_lsn());
    return ok();
}

} // namespace Calico