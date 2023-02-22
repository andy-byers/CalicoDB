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

auto Recovery::start() -> Status
{
    const auto &set = m_wal->m_set;

    if (set.first().is_null()) {
        return Status::ok();
    }

    WalReader *temp;
    Calico_Try(m_wal->new_reader(&temp));
    std::unique_ptr<WalReader> reader {temp};
    Lsn last_lsn;
    Status s;

    const auto translate_status = [&s, &reader, last = set.last()] {
        CALICO_EXPECT_FALSE(s.is_ok());
        if (s.is_not_found() || s.is_corruption()) {
            if (reader->id() == last) {
                s = Status::ok();
            }
        }
        return s;
    };

    // Skip updates that are already in the database.
    s = reader->seek(m_pager->recovery_lsn());
    if (s.is_not_found()) {
        s = Status::ok();
    }

    // Roll forward and apply missing updates.
    for (Size i {}; ; i++) {
        WalPayloadOut payload;
        s = reader->read(payload);
        if (!s.is_ok()) {
            break;
        }
        const auto decoded = decode_payload(payload);

        // Payload has an invalid type.
        if (std::holds_alternative<std::monostate>(decoded)) {
            return Status::corruption("wal is corrupted");
        }

        if (i && last_lsn.value + 1 != payload.lsn().value) {
            return Status::corruption("missing wal record");
        }
        last_lsn = payload.lsn();

        if (std::holds_alternative<CommitDescriptor>(decoded)) {
            *m_commit_lsn = payload.lsn();
        } else if (std::holds_alternative<DeltaDescriptor>(decoded)) {
            const auto delta = std::get<DeltaDescriptor>(decoded);
            Calico_Try(with_page(*m_pager, delta, [this, &delta](auto &page) {
                if (delta.lsn > read_page_lsn(page)) {
                    m_pager->upgrade(page);
                    apply_redo(page, delta);
                }
            }));
        }
    }

    // The reader either hit the end of the WAL or errored out. It may have encountered a corrupted or incomplete
    // last record if the database crashed while in the middle of writing that record.
    Calico_Try(translate_status());

    if (*m_commit_lsn == last_lsn) {
        return Status::ok();
    }

    // Put the reader at the segment right after the most-recent commit. We can read the last transaction forward
    // to revert it, because the full image records are disjoint w.r.t. the pages they reference.
    Calico_Try(reader->seek(*m_commit_lsn));
    Calico_Try(reader->skip());

    for (; ; ) {
        WalPayloadOut payload;
        s = reader->read(payload);
        if (!s.is_ok()) {
            break;
        }
        const auto decoded = decode_payload(payload);

        if (std::holds_alternative<std::monostate>(decoded)) {
            return Status::corruption("wal is corrupted");
        }
        if (std::holds_alternative<FullImageDescriptor>(decoded)) {
            const auto image = std::get<FullImageDescriptor>(decoded);
            Calico_Try(with_page(*m_pager, image, [this, &image](auto &page) {
                m_pager->upgrade(page);
                apply_undo(page, image);
            }));
        }
    }

    return translate_status();
}

auto Recovery::finish() -> Status
{
    Calico_Try(m_pager->flush({}));
    Calico_Try(m_wal->truncate(*m_commit_lsn));
    Calico_Try(m_wal->start_writing());
    m_wal->cleanup(m_pager->recovery_lsn());

    // Make sure the file size matches the header page count, which should be correct if we made it this far.
    return m_pager->truncate(m_pager->page_count());
}

} // namespace Calico