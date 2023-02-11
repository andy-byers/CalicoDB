#include "pager.h"
#include "frame_buffer.h"
#include "page.h"
#include "tree/header.h"
#include "utils/logging.h"
#include "utils/types.h"
#include "wal/wal.h"
#include <thread>

namespace Calico {

namespace fs = std::filesystem;

static constexpr Id MAX_ID {std::numeric_limits<Size>::max()};

#define MAYBE_ERROR(expr) \
    do { \
        if (auto calico_s = (expr); !calico_s.is_ok()) { \
            m_error->set(std::move(calico_s)); \
        } \
    } while (0)

auto Pager::open(const Parameters &param) -> tl::expected<Pager::Ptr, Status>
{
    auto framer = FrameBuffer::open(
        param.prefix,
        param.storage,
        param.page_size,
        param.frame_count);
    if (!framer.has_value()) {
        return tl::make_unexpected(framer.error());
    }

    if (auto ptr = Pager::Ptr {new (std::nothrow) Pager {param, std::move(*framer)}}) {
        return ptr;
    }
    return tl::make_unexpected(Status::system_error("could not allocate pager object: out of memory"));
}

Pager::Pager(const Parameters &param, FrameBuffer framer)
    : m_framer {std::move(framer)},
      m_commit_lsn {param.commit_lsn},
      m_in_txn {param.in_txn},
      m_status {param.status},
      m_scratch {param.scratch},
      m_wal {param.wal}
{
    CALICO_EXPECT_NE(m_status, nullptr);
    CALICO_EXPECT_NE(m_scratch, nullptr);
    CALICO_EXPECT_NE(m_wal, nullptr);
}

auto Pager::bytes_written() const -> Size
{
    return m_framer.bytes_written();
}

auto Pager::page_count() const -> Size
{
    return m_framer.page_count();
}

auto Pager::page_size() const -> Size
{
    return m_framer.page_size();
}

auto Pager::hit_ratio() const -> double
{
    return m_registry.hit_ratio();
}

auto Pager::pin_frame(Id pid) -> Status
{
    CALICO_EXPECT_FALSE(m_registry.contains(pid));

    if (!m_framer.available()) {
        if (const auto success = try_make_available()) {
            if (!*success) {
                logv(m_info_log, "out of frames");

                // This call blocks, so the WAL will be caught up when it returns. The recursive call to
                // pin_frame() should succeed.
                Calico_Try_S(m_wal->flush());
                return pin_frame(pid);
            }
        } else {
            return success.error();
        }
    }
    if (const auto index = m_framer.pin(pid)) {
        // Associate the page ID with the frame index we got from the framer.
        m_registry.put(pid, {*index});
        return Status::ok();
    } else {
        return index.error();
    }
}

auto Pager::clean_page(PageCache::Entry &entry) -> PageList::Iterator
{
    auto token = *entry.token;
    // Reset the dirty list reference.
    entry.token.reset();
    return m_dirty.remove(token);
}

auto Pager::set_recovery_lsn(Lsn lsn) -> void
{
    CALICO_EXPECT_LE(m_recovery_lsn, lsn);
    m_recovery_lsn = lsn;
}

auto Pager::sync() -> Status
{
    return m_framer.sync();
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
        CALICO_EXPECT_TRUE(m_registry.contains(page_id));
        auto &entry = m_registry.get(page_id)->value;
        const auto frame_id = entry.index;
        const auto page_lsn = m_framer.get_frame(frame_id).lsn();

        if (largest < page_lsn) {
            largest = page_lsn;
        }

        if (page_id.as_index() >= m_framer.page_count()) {
            // Page is out of range (abort was called and the database got smaller).
            m_registry.erase(page_id);
            m_framer.unpin(frame_id);
            itr = m_dirty.remove(itr);
        } else if (page_lsn > m_wal->flushed_lsn()) {
            // WAL record referencing this page has not been flushed yet.
            logv(m_info_log, "could not flush page ",
                 page_id.value, ": updates for lsn ",
                 page_lsn.value, " are not in the wal");
            itr = next(itr);
        } else if (record_lsn <= target_lsn) {
            // Flush the page.
            auto s = m_framer.write_back(frame_id);

            // Advance to the next dirty list entry.
            itr = clean_page(entry); // TODO: We will clean the page regardless of error.
            Calico_Try_S(s);
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
        set_recovery_lsn(target_lsn);
    }
    return Status::ok();
}

auto Pager::recovery_lsn() -> Id
{
    auto lowest = MAX_ID;
    for (auto entry: m_dirty) {
        if (lowest > entry.record_lsn) {
            lowest = entry.record_lsn;
        }
    }
    // NOTE: Recovery LSN is not always up-to-date. We update it here and in flush(), making sure it is monotonically increasing.
    if (lowest != MAX_ID && m_recovery_lsn < lowest) {
        set_recovery_lsn(lowest);
    }
    return m_recovery_lsn;
}

auto Pager::try_make_available() -> tl::expected<bool, Status>
{
    Id page_id;
    auto evicted = m_registry.evict([this, &page_id](auto pid, auto entry) {
        const auto &frame = m_framer.get_frame(entry.index);
        page_id = pid;

        // The page/frame management happens under lock, so if this frame is not referenced, it is safe to evict.
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
            *m_status = std::move(s);
        }
        return false;
    }

    auto &[frame_index, dirty_token] = *evicted;
    auto s = Status::ok();

    if (dirty_token) {
        // NOTE: We don't update the record LSN field because we are getting rid of this page.
        s = m_framer.write_back(frame_index);
        clean_page(*evicted);
    }
    m_framer.unpin(frame_index);
    if (!s.is_ok()) {
        return tl::make_unexpected(s);
    }
    return true;
}

auto Pager::watch_page(Page &page, PageCache::Entry &entry) -> void
{
    // This function needs external synchronization!
    CALICO_EXPECT_GT(m_framer.ref_sum(), 0);
    const auto lsn = read_page_lsn(page);

    // Make sure this page is in the dirty list. LSN is saved to determine when the page should be written back.
    if (!entry.token.has_value()) {
        entry.token = m_dirty.insert(page.id(), lsn);
    }

    if (*m_in_txn && lsn <= *m_commit_lsn) {
        const auto next_lsn = m_wal->current_lsn();
        m_wal->log(encode_full_image_payload(
            next_lsn, page.id(), page.view(0), *m_scratch->get()));
        write_page_lsn(page, next_lsn);
    }
}

auto Pager::allocate() -> tl::expected<Page, Status>
{
    return acquire(Id::from_index(m_framer.page_count()));
}

auto Pager::acquire(Id pid) -> tl::expected<Page, Status>
{
    const auto do_acquire = [this](auto &entry) -> tl::expected<Page, Status> {
        auto page = m_framer.ref(entry.index);

        // Write back pages that are too old. This is so that old WAL segments can be removed.
        if (entry.token) {
            const auto lsn = read_page_lsn(page);
            const auto checkpoint = (*entry.token)->record_lsn;
            const auto cutoff = *m_commit_lsn;

            if (checkpoint <= cutoff && lsn <= m_wal->flushed_lsn()) {
                auto s = m_framer.write_back(entry.index);

                if (s.is_ok()) {
                    clean_page(entry);
                } else {
                    *m_status = std::move(s);
                    return tl::make_unexpected(*m_status);
                }
            }
        }
        return page;
    };

    CALICO_EXPECT_FALSE(pid.is_null());

    if (auto itr = m_registry.get(pid); itr != m_registry.end()) {
        return do_acquire(itr->value);
    }

    if (auto s = pin_frame(pid); !s.is_ok()) {
        return tl::make_unexpected(std::move(s));
    }

    CALICO_EXPECT_TRUE(m_registry.contains(pid));
    return do_acquire(m_registry.get(pid)->value);
}

auto Pager::upgrade(Page &page) -> void
{
    auto itr = m_registry.get(page.id());
    CALICO_EXPECT_NE(itr, m_registry.end());
    m_framer.upgrade(itr->value.index, page);
    watch_page(page, itr->value);
}

auto Pager::release(Page page) -> void
{
    if (page.is_writable() && *m_in_txn) {
        const auto next_lsn = m_wal->current_lsn();
        write_page_lsn(page, next_lsn);
        m_wal->log(encode_deltas_payload(
            next_lsn, page.id(), page.view(0),
            page.deltas(), *m_scratch->get()));
    }
    CALICO_EXPECT_GT(m_framer.ref_sum(), 0);

    // This page must already be acquired.
    auto itr = m_registry.get(page.id());
    CALICO_EXPECT_NE(itr, m_registry.end());
    auto &entry = itr->value;
    m_framer.unref(entry.index, std::move(page));
}

auto Pager::save_state(FileHeader &header) -> void
{
    header.recovery_lsn = m_recovery_lsn;
    m_framer.save_state(header);
}

auto Pager::load_state(const FileHeader &header) -> void
{
    if (m_recovery_lsn.value < header.recovery_lsn.value) {
        set_recovery_lsn(Id {header.recovery_lsn});
    }
    m_framer.load_state(header);
}

#undef MAYBE_ERROR

} // namespace Calico
