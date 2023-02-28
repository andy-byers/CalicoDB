#include "recovery.h"
#include "page.h"
#include "pager.h"
#include "wal.h"
#include "wal_reader.h"

namespace calicodb
{

static auto
apply_undo(Page &page, const FullImageDescriptor &image)
{
    const auto data = image.image;
    mem_copy(page.span(0, data.size()), data);
    if (page.size() > data.size()) {
        mem_clear(page.span(data.size(), page.size() - data.size()));
    }
}

static auto
apply_redo(Page &page, const DeltaDescriptor &deltas)
{
    for (auto [offset, data] : deltas.deltas) {
        mem_copy(page.span(offset, data.size()), data);
    }
}

static auto
is_commit(const DeltaDescriptor &deltas)
{
    return deltas.pid.is_root() && deltas.deltas.size() == 1 && deltas.deltas.front().offset == 0 &&
           deltas.deltas.front().data.size() == FileHeader::SIZE + sizeof(Lsn);
}

template <class Descriptor, class Callback>
static auto
with_page(Pager &pager, const Descriptor &descriptor, const Callback &callback)
{
    Page page;
    CDB_TRY(pager.acquire(descriptor.pid, page));

    callback(page);
    pager.release(std::move(page));
    return Status::ok();
}

Recovery::Recovery(Pager &pager, WriteAheadLog &wal, Lsn &commit_lsn)
    : m_reader_data(wal_scratch_size(pager.page_size()), '\x00'), m_reader_tail(wal_block_size(pager.page_size()), '\x00'), m_pager {&pager}, m_storage {wal.m_storage}, m_set {&wal.m_set}, m_wal {&wal}, m_commit_lsn {&commit_lsn}
{
}

auto Recovery::open_reader(Id segment, std::unique_ptr<Reader> &out) -> Status
{
    Reader *file;
    const auto name = encode_segment_name(m_wal->m_prefix, segment);
    CDB_TRY(m_storage->new_reader(name, &file));
    out.reset(file);
    return Status::ok();
}

auto Recovery::recover() -> Status
{
    CDB_TRY(recover_phase_1());
    return recover_phase_2();
}

/* Recovery routine. This routine is run on startup, and is meant to ensure that
 * the database is in a consistent state. If any WAL segments are found
 * containing updates that are not present in the database, these segments are
 * read and the updates applied. If the final transaction is missing a commit
 * record, then those updates are reverted and the log is truncated.
 */
auto Recovery::recover_phase_1() -> Status
{
    if (m_set->is_empty()) {
        return Status::ok();
    }

    // We are starting up the database, so these should be set now. They may be
    // updated if we find a commit record in the WAL past what was applied to
    // the database.
    if (m_wal->current_lsn().is_null()) {
        m_wal->m_last_lsn = *m_commit_lsn;
        m_wal->m_flushed_lsn = *m_commit_lsn;
        m_pager->m_recovery_lsn = *m_commit_lsn;
    }

    std::unique_ptr<Reader> file;
    auto segment = m_set->first();
    auto commit_lsn = *m_commit_lsn;
    auto commit_segment = segment;
    Lsn last_lsn;

    const auto translate_status = [&segment, this](auto s, Lsn lsn) {
        CDB_EXPECT_FALSE(s.is_ok());
        if (s.is_corruption()) {
            // Allow corruption/incomplete records on the last segment, past the
            // most-recent successful commit.
            if (segment == m_set->last() && lsn >= *m_commit_lsn) {
                return Status::ok();
            }
        }
        return s;
    };

    const auto redo = [&](const auto &payload) {
        const auto decoded = decode_payload(payload);
        if (std::holds_alternative<DeltaDescriptor>(decoded)) {
            const auto deltas = std::get<DeltaDescriptor>(decoded);
            if (is_commit(deltas)) {
                commit_lsn = deltas.lsn;
                commit_segment = segment;
            }
            // WARNING: Applying these updates can cause the in-memory file
            // header variables to be incorrect. This
            //          must be fixed by the caller after this method returns.
            return with_page(*m_pager, deltas, [this, &deltas](auto &page) {
                if (read_page_lsn(page) < deltas.lsn) {
                    m_pager->upgrade(page);
                    apply_redo(page, deltas);
                }
            });
        } else if (std::holds_alternative<std::monostate>(decoded)) {
            CDB_TRY(translate_status(Status::corruption("wal is corrupted"), last_lsn));
            return Status::not_found("finished");
        }
        return Status::ok();
    };

    const auto undo = [&](const auto &payload) {
        const auto decoded = decode_payload(payload);
        if (std::holds_alternative<FullImageDescriptor>(decoded)) {
            const auto image = std::get<FullImageDescriptor>(decoded);
            return with_page(*m_pager, image, [this, &image](auto &page) {
                if (read_page_lsn(page) > image.lsn && image.lsn > *m_commit_lsn) {
                    m_pager->upgrade(page);
                    apply_undo(page, image);
                }
            });
        } else if (std::holds_alternative<std::monostate>(decoded)) {
            CDB_TRY(translate_status(Status::corruption("wal is corrupted"), last_lsn));
            return Status::not_found("finished");
        }
        return Status::ok();
    };

    const auto roll = [&](const auto &action) {
        CDB_TRY(open_reader(segment, file));
        WalReader reader {*file, m_reader_tail};

        for (;;) {
            Span buffer {m_reader_data};
            auto s = reader.read(buffer);

            if (s.is_not_found()) {
                break;
            } else if (!s.is_ok()) {
                CDB_TRY(translate_status(s, last_lsn));
                return Status::ok();
            }

            WalPayloadOut payload {buffer};
            last_lsn = payload.lsn();

            s = action(payload);
            if (s.is_not_found()) {
                break;
            } else if (!s.is_ok()) {
                return s;
            }
        }
        return Status::ok();
    };

    /* Roll forward, applying missing updates until we reach the end. The final
     * segment may contain a partial/corrupted record.
     */
    for (;; segment = m_set->id_after(segment)) {
        CDB_TRY(roll(redo));
        if (segment == m_set->last()) {
            break;
        }
    }

    // Didn't make it to the end of the WAL.
    if (segment != m_set->last()) {
        return Status::corruption("wal could not be read to the end");
    }

    if (last_lsn == commit_lsn) {
        if (*m_commit_lsn <= commit_lsn) {
            *m_commit_lsn = commit_lsn;
            return Status::ok();
        } else {
            return Status::corruption("missing commit record");
        }
    }
    *m_commit_lsn = commit_lsn;

    /* Roll backward, reverting misapplied updates until we reach the
     * most-recent commit. We are able to read the log forward, since the full
     * images are disjoint. Again, the last segment we read may contain a
     * partial/corrupted record.
     */
    segment = commit_segment;
    for (; !segment.is_null(); segment = m_set->id_after(segment)) {
        CDB_TRY(roll(undo));
    }
    return Status::ok();
}

auto Recovery::recover_phase_2() -> Status
{
    // Pager needs the updated state to determine the page count.
    Page page;
    CDB_TRY(m_pager->acquire(Id::root(), page));
    FileHeader header {page};
    m_pager->load_state(header);
    m_pager->release(std::move(page));

    // TODO: This is too expensive for large databases. Look into a WAL index?
    // Make sure we aren't missing any WAL records.
    // for (auto id = Id::root(); id.value <= m_pager->page_count(); id.value++)
    // {
    //    Calico_Try(m_pager->acquire(Id::root(), page));
    //    const auto lsn = read_page_lsn(page);
    //    m_pager->release(std::move(page));
    //
    //    if (lsn > *m_commit_lsn) {
    //        return Status::corruption("missing wal updates");
    //    }
    //}

    /* Make sure all changes have made it to disk, then remove WAL segments from
     * the right.
     */
    CDB_TRY(m_pager->flush());
    for (auto id = m_set->last(); !id.is_null(); id = m_set->id_before(id)) {
        CDB_TRY(m_storage->remove_file(encode_segment_name(m_wal->m_prefix, id)));
    }
    m_set->remove_after(Id::null());

    m_wal->m_last_lsn = *m_commit_lsn;
    m_wal->m_flushed_lsn = *m_commit_lsn;
    m_pager->m_recovery_lsn = *m_commit_lsn;

    // Make sure the file size matches the header page count, which should be
    // correct if we made it this far.
    CDB_TRY(m_pager->truncate(m_pager->page_count()));
    return m_pager->sync();
}

} // namespace calicodb