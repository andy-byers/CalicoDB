#include "recovery.h"
#include "pager/page.h"
#include "pager/pager.h"
#include "wal/reader.h"

namespace Calico {

static auto apply_undo(Page &page, const FullImageDescriptor &image)
{
    const auto data = image.image;
    mem_copy(page.span(0, data.size()), data);
    if (page.size() > data.size()) {
        mem_clear(page.span(data.size(), page.size() - data.size()));
    }
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
    Page page;
    Calico_Try(pager.acquire(descriptor.pid, page));

    callback(page);
    pager.release(std::move(page));
    return Status::ok();
}

Recovery::Recovery(Pager &pager, WriteAheadLog &wal, Lsn &commit_lsn)
    : m_reader_data(wal_scratch_size(pager.page_size()), '\x00'),
      m_reader_tail(wal_block_size(pager.page_size()), '\x00'),
      m_pager {&pager},
      m_wal {&wal},
      m_commit_lsn {&commit_lsn}
{}

auto Recovery::open_reader(Id segment, std::unique_ptr<Reader> &out) -> Status
{
    Reader *file;
    const auto name = encode_segment_name(m_wal->m_prefix, segment);
    Calico_Try(m_pager->m_storage->new_reader(name, &file));
    out.reset(file);
    return Status::ok();
}

auto Recovery::recover() -> Status
{
    Calico_Try(recover_phase_1());
    return recover_phase_2();
}

/* Recovery routine. This routine is run on startup, and is meant to ensure that the database is in a
 * consistent state. If any WAL segments are found containing updates that are not present in the
 * database, these segments are read and the updates applied. If the final transaction is missing a
 * commit record, then those updates are reverted and the log is truncated.
 */
auto Recovery::recover_phase_1() -> Status
{
    auto &set = m_wal->m_set;
    auto &storage = *m_pager->m_storage;

    if (set.is_empty()) {
        return Status::ok();
    }

    Size commit_offset {};
    Id commit_segment;
    Lsn commit_lsn;
    Lsn last_lsn;

    std::unique_ptr<Reader> file;
    auto segment = set.first();

    const auto translate_status = [&, last = set.last()](auto s, Lsn lsn) {
        CALICO_EXPECT_FALSE(s.is_ok());
        if (s.is_corruption()) {
            // Allow corruption/incomplete records on the last segment, past the most-recent successful commit.
            if (segment == last && lsn >= *m_commit_lsn) {
                return Status::ok();
            }
        }
        return s;
    };

    const auto redo = [&](const auto &payload, auto offset) {
        const auto decoded = decode_payload(payload);
        if (std::holds_alternative<DeltaDescriptor>(decoded)) {
            const auto deltas = std::get<DeltaDescriptor>(decoded);
            return with_page(*m_pager, deltas, [this, &deltas](auto &page) {
                if (read_page_lsn(page) < deltas.lsn) {
                    m_pager->upgrade(page);
                    apply_redo(page, deltas);
                }
            });
        } else if (std::holds_alternative<CommitDescriptor>(decoded)) {
            const auto commit = std::get<CommitDescriptor>(decoded);
            commit_lsn = commit.lsn;
            commit_offset = offset;
            commit_segment = segment;
        } else if (std::holds_alternative<std::monostate>(decoded)) {
            Calico_Try(translate_status(Status::corruption("wal is corrupted"), last_lsn));
            return Status::not_found("finished");
        }
        return Status::ok();
    };

    const auto undo = [&](const auto &payload, auto) {
        const auto decoded = decode_payload(payload);
        if (std::holds_alternative<FullImageDescriptor>(decoded)) {
            const auto image = std::get<FullImageDescriptor>(decoded);
            return with_page(*m_pager, image, [this, &image](auto &page) {
                if (read_page_lsn(page) >= image.lsn) {
                    m_pager->upgrade(page);
                    apply_undo(page, image);
                }
            });
        } else if (std::holds_alternative<std::monostate>(decoded)) {
            Calico_Try(translate_status(Status::corruption("wal is corrupted"), last_lsn));
            return Status::not_found("finished");
        }
        return Status::ok();
    };

    const auto roll = [&](const auto &action) {
        Calico_Try(open_reader(segment, file));
        WalReader reader {*file, m_reader_tail};

        for (; ; ) {
            Span buffer {m_reader_data};
            auto s = reader.read(buffer);

            if (s.is_not_found()) {
                break;
            } else if (!s.is_ok()) {
                Calico_Try(translate_status(s, last_lsn));
            }

            WalPayloadOut payload {buffer};
            last_lsn = payload.lsn();

            s = action(payload, reader.offset());
            if (s.is_not_found()) {
                break;
            } else if (!s.is_ok()) {
                return s;
            }
        }
        return Status::ok();
    };

    /* Roll forward, applying missing updates until we reach the end. The final segment may contain
     * a partial/corrupted record.
     */
    for (; ; segment = set.id_after(segment)) {
        Calico_Try(roll(redo));
        if (segment == set.last()) {
            break;
        }
    }

    // Didn't make it to the end of the WAL.
    if (segment != set.last()) {
        return Status::corruption("wal could not be read");
    }

    if (last_lsn == commit_lsn) {
        if (*m_commit_lsn <= commit_lsn) {
            *m_commit_lsn = commit_lsn;
            return m_pager->flush({});
        } else {
            return Status::corruption("wal could not be read");
        }
    }

    /* Roll backward, reverting misapplied updates until we reach either the beginning, or the saved
     * commit offset. The first segment we read may contain a partial/corrupted record.
     */
    segment = commit_segment;
    for (; !segment.is_null(); segment = set.id_after(segment)) {
        Calico_Try(roll(undo));
    }

    /* Make sure all changes have made it to disk, then remove WAL segments from the right. Once we
     * hit the segment containing the most-recent commit record, truncate the file, respecting the
     * fact that the log file length must be a multiple of the block size.
     */
    Calico_Try(m_pager->flush({}));
    segment = set.last();
    for (; !segment.is_null(); segment = set.id_before(segment)) {
        const auto name = encode_segment_name(m_wal->m_prefix, segment);
        if (segment == commit_segment) {
            const auto block_number = commit_offset / m_reader_tail.size();
            const auto block_end = (block_number+1) * m_reader_tail.size();
            Calico_Try(storage.resize_file(name, commit_offset));
            // TODO: If this call fails, we will need to fix the database in Database::repair().
            Calico_Try(storage.resize_file(name, block_end));
            break;
        }
        // This whole segment must belong to the transaction we are reverting.
        Calico_Try(storage.remove_file(name));
    }
    set.remove_after(segment);
    m_wal->m_last_lsn = *m_commit_lsn;
    m_wal->m_flushed_lsn = *m_commit_lsn;
    return Status::ok();
}

auto Recovery::recover_phase_2() -> Status
{
    Calico_Try(m_wal->start_writing());
    m_pager->m_recovery_lsn = *m_commit_lsn;
    m_wal->cleanup( *m_commit_lsn);

    // Make sure the file size matches the header page count, which should be correct if we made it this far.
    return m_pager->truncate(m_pager->page_count());
}

} // namespace Calico