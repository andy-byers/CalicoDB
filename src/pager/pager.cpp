#include "pager.h"
#include <limits>
#include "frames.h"
#include "page.h"
#include "tree/header.h"
#include "utils/logging.h"
#include "utils/types.h"
#include "wal/wal.h"

namespace Calico {

static constexpr Id MAX_ID {std::numeric_limits<Size>::max()};

auto Pager::open(const Parameters &param, Pager **out) -> Status
{
    CALICO_EXPECT_TRUE(is_power_of_two(param.page_size));
    CALICO_EXPECT_GE(param.page_size, MINIMUM_PAGE_SIZE);
    CALICO_EXPECT_LE(param.page_size, MAXIMUM_PAGE_SIZE);

    Editor *file;
    Calico_Try(param.storage->new_editor(param.prefix + "data", &file));

    // Allocate the frames, i.e. where pages from disk are stored in memory. Aligned to the page size, so it could
    // potentially be used for direct I/O.
    AlignedBuffer buffer {
        param.page_size * param.frame_count,
        param.page_size};
    if (buffer.get() == nullptr) {
        return Status::system_error("out of memory");
    }

    auto *ptr = new (std::nothrow) Pager {param, file, std::move(buffer)};
    if (ptr == nullptr) {
        return Status::system_error("out of memory");
    }
    *out = ptr;
    return Status::ok();
}

Pager::Pager(const Parameters &param, Editor *file, AlignedBuffer buffer)
    : m_path {param.prefix + "data"},
      m_frames {file, std::move(buffer), param.page_size, param.frame_count},
      m_commit_lsn {param.commit_lsn},
      m_in_txn {param.in_txn},
      m_status {param.status},
      m_scratch {param.scratch},
      m_wal {param.wal},
      m_storage {param.storage}
{
    CALICO_EXPECT_NE(m_status, nullptr);
    CALICO_EXPECT_NE(m_scratch, nullptr);
    CALICO_EXPECT_NE(m_wal, nullptr);
}

auto Pager::bytes_written() const -> Size
{
    return m_frames.bytes_written();
}

auto Pager::page_count() const -> Size
{
    return m_frames.page_count();
}

auto Pager::page_size() const -> Size
{
    return m_frames.page_size();
}

auto Pager::hit_ratio() const -> double
{
    return m_cache.hit_ratio();
}

auto Pager::pin_frame(Id pid) -> Status
{
    if (auto s = do_pin_frame(pid); s.is_not_found()) {
        logv(m_info_log, s.what().data());
        Calico_Try(m_wal->flush());
        return do_pin_frame(pid);
    } else {
        return s;
    }
}

auto Pager::do_pin_frame(Id pid) -> Status
{
    CALICO_EXPECT_FALSE(m_cache.contains(pid));

    if (!m_frames.available()) {
        if (!make_frame_available()) {
            Calico_Try(*m_status);
            logv(m_info_log, "out of frames");
            Calico_Try(m_wal->flush());
            return Status::not_found("out of frames");
        }
    }

    Size fid;
    Calico_Try(m_frames.pin(pid, fid));

    // Associate the page ID with the frame index we got from the framer.
    m_cache.put(pid, {fid});
    return Status::ok();
}

auto Pager::clean_page(PageCache::Entry &entry) -> PageList::Iterator
{
    auto token = *entry.token;
    // Reset the dirty list reference.
    entry.token.reset();
    return m_dirty.remove(token);
}

auto Pager::sync() -> Status
{
    return m_frames.sync();
}

auto Pager::flush(Lsn target_lsn) -> Status
{
    // An LSN of NULL causes all pages to be flushed.
    if (target_lsn.is_null()) {
        target_lsn = MAX_ID;
    }

    auto largest = Id::null();
    for (auto itr = m_dirty.begin(); itr != m_dirty.end(); ) {
        const auto [page_id, record_lsn] = *itr;
        CALICO_EXPECT_TRUE(m_cache.contains(page_id));
        auto &entry = m_cache.get(page_id)->value;
        const auto frame_id = entry.index;
        const auto page_lsn = m_frames.get_frame(frame_id).lsn();

        if (largest < page_lsn) {
            largest = page_lsn;
        }

        if (page_id.as_index() >= m_frames.page_count()) {
            // Page is out of range.
            m_cache.erase(page_id);
            m_frames.unpin(frame_id);
            itr = m_dirty.remove(itr);
        } else if (page_lsn > m_wal->flushed_lsn()) {
            // WAL record referencing this page has not been flushed yet.
            logv(m_info_log, "could not flush page ",
                 page_id.value, ": updates for lsn ",
                 page_lsn.value, " are not in the wal");
            itr = next(itr);
        } else if (record_lsn <= target_lsn) {
            // Flush the page.
            auto s = m_frames.write_back(frame_id);

            // Advance to the next dirty list entry.
            itr = clean_page(entry);
            Calico_Try(s);
        } else {
            itr = next(itr);
        }
    }

    // We have flushed the entire cache.
    if (target_lsn == MAX_ID) {
        target_lsn = largest;
    }

    // We don't have any pages in memory with LSNs below this value.
    if (m_recovery_lsn < target_lsn) {
        m_recovery_lsn = target_lsn;
    }
    return Status::ok();
}

auto Pager::recovery_lsn() -> Id
{
    return m_recovery_lsn;
}

auto Pager::make_frame_available() -> bool
{
    Id page_id;
    auto evicted = m_cache.evict([this, &page_id](auto pid, auto entry) {
        const auto &frame = m_frames.get_frame(entry.index);
        page_id = pid;

        if (frame.ref_count() != 0) {
            return false;
        }
        if (!entry.token) {
            return true;
        }
        if (*m_in_txn) {
            return frame.lsn() <= m_wal->flushed_lsn();
        }
        return true;
    });

    if (!evicted.has_value()) {
        if (auto s = m_wal->flush(); !s.is_ok()) {
            *m_status = s;
        }
        return false;
    }

    auto &[frame_index, dirty_token] = *evicted;
    auto s = Status::ok();

    if (dirty_token) {
        // NOTE: We don't update the record LSN field because we are getting rid of this page.
        s = m_frames.write_back(frame_index);
        clean_page(*evicted);
    }
    m_frames.unpin(frame_index);
    if (!s.is_ok()) {
        *m_status = s;
        return false;
    }
    return true;
}

auto Pager::watch_page(Page &page, PageCache::Entry &entry, int important) -> void
{
    CALICO_EXPECT_GT(m_frames.ref_sum(), 0);
    const auto lsn = read_page_lsn(page);

    // The "important" parameter should be used when we don't need to track the before contents of the whole
    // page. For example, when allocating a page from the freelist, we only care about the page LSN stored in
    // the first 8 bytes; the rest is junk.
    Size watch_size;
    if (important < 0) {
        watch_size = page.size();
    } else {
        watch_size = static_cast<Size>(important);
    }

    // Make sure this page is in the dirty list. This is one place where the "record LSN" is set.
    if (!entry.token.has_value()) {
        entry.token = m_dirty.insert(page.id(), lsn);
    }

    // We only write a full image if the WAL does not already contain one for this page. If the page was modified
    // during this transaction, then we already have one written.
    if (*m_in_txn && lsn <= *m_commit_lsn) {
        const auto next_lsn = m_wal->current_lsn();
        const auto image = page.view(0, watch_size);
        auto s = m_wal->log(encode_full_image_payload(
            next_lsn, page.id(), image, *m_scratch));

        if (s.is_ok()) {
            write_page_lsn(page, next_lsn);
        } else {
            *m_status = s;
        }
    }
}

auto Pager::allocate(Page &page) -> Status
{
    Calico_Try(acquire(Id::from_index(m_frames.page_count()), page));
    upgrade(page, 0);
    return Status::ok();
}

auto Pager::acquire(Id pid, Page &page) -> Status
{
    CALICO_EXPECT_FALSE(pid.is_null());

    const auto do_acquire = [this, &page](auto &entry) {
        m_frames.ref(entry.index, page);

        // Write back pages that are too old. This is so that old WAL segments can be removed.
        if (entry.token) {
            const auto lsn = read_page_lsn(page);
            const auto checkpoint = (*entry.token)->record_lsn;
            const auto cutoff = *m_commit_lsn;

            if (checkpoint <= cutoff && lsn <= m_wal->flushed_lsn()) {
                auto s = m_frames.write_back(entry.index);

                if (s.is_ok()) {
                    clean_page(entry);
                } else {
                    *m_status = s;
                    return s;
                }
            }
        }
        return Status::ok();
    };

    if (auto itr = m_cache.get(pid); itr != m_cache.end()) {
        return do_acquire(itr->value);
    }

    Calico_Try(pin_frame(pid));
    CALICO_EXPECT_TRUE(m_cache.contains(pid));
    return do_acquire(m_cache.get(pid)->value);
}

auto Pager::upgrade(Page &page, int important) -> void
{
    CALICO_EXPECT_LE(important, static_cast<int>(page.size()));
    auto itr = m_cache.get(page.id());
    CALICO_EXPECT_NE(itr, m_cache.end());
    m_frames.upgrade(itr->value.index, page);
    watch_page(page, itr->value, important);
}

auto Pager::release(Page page) -> void
{
    CALICO_EXPECT_GT(m_frames.ref_sum(), 0);
    CALICO_EXPECT_TRUE(m_cache.contains(page.id()));
    auto [index, token] = m_cache.get(page.id())->value;

    if (page.is_writable() && *m_in_txn) {
        const auto next_lsn = m_wal->current_lsn();
        write_page_lsn(page, next_lsn);
        auto s = m_wal->log(encode_deltas_payload(
            next_lsn, page.id(), page.view(0),
            page.deltas(), *m_scratch));
        if (!s.is_ok()) {
            *m_status = s;
        }
    }
    m_frames.unref(index, std::move(page));
}

auto Pager::truncate(Size page_count) -> Status
{
    CALICO_EXPECT_GT(page_count, 0);
    Calico_Try(m_storage->resize_file(m_path, page_count * m_frames.page_size()));
    m_frames.m_page_count = page_count;

    const auto predicate = [this](auto pid, auto) {
        return pid.value > m_frames.page_count();
    };
    std::optional<PageCache::Entry> entry;
    while ((entry = m_cache.evict(predicate))) {
        m_frames.unpin(entry->index);
        if (entry->token) {
            m_dirty.remove(*entry->token);
        }
    }
    return flush({});
}

auto Pager::save_state(FileHeader &header) -> void
{
    m_frames.save_state(header);
}

auto Pager::load_state(const FileHeader &header) -> void
{
    m_frames.load_state(header);
}

} // namespace Calico
