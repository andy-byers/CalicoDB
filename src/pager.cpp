// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "pager.h"
#include "db_impl.h"
#include "frames.h"
#include "header.h"
#include "logging.h"
#include "page.h"
#include "types.h"
#include "wal.h"
#include <limits>

namespace calicodb
{

#define SET_STATUS(s)                  \
    do {                               \
        if (m_state->status.is_ok()) { \
            m_state->status = s;       \
        }                              \
    } while (0)

static constexpr Id kMaxId {std::numeric_limits<std::size_t>::max()};

auto Pager::open(const Parameters &param, Pager **out) -> Status
{
    CDB_EXPECT_TRUE(is_power_of_two(param.page_size));
    CDB_EXPECT_GE(param.page_size, kMinPageSize);
    CDB_EXPECT_LE(param.page_size, kMaxPageSize);
    CDB_EXPECT_GE(param.frame_count, kMinFrameCount);
    CDB_EXPECT_LE(param.page_size * param.frame_count, kMaxCacheSize);

    Editor *file;
    CDB_TRY(param.env->new_editor(param.filename, file));

    // Allocate the frames, i.e. where pages from disk are stored in memory. Aligned to the page size, so it could
    // potentially be used for direct I/O.
    AlignedBuffer buffer {
        param.page_size * param.frame_count,
        param.page_size};
    if (buffer.get() == nullptr) {
        return Status::io_error("out of memory");
    }

    auto *ptr = new Pager {param, *file, std::move(buffer)};
    *out = ptr;
    return Status::ok();
}

Pager::Pager(const Parameters &param, Editor &file, AlignedBuffer buffer)
    : m_filename {param.filename},
      m_frames {file, std::move(buffer), param.page_size, param.frame_count},
      m_wal {param.wal},
      m_env {param.env},
      m_info_log {param.info_log},
      m_state {param.state}
{
    CDB_EXPECT_NE(m_wal, nullptr);
    CDB_EXPECT_NE(m_state, nullptr);
}

auto Pager::bytes_read() const -> std::size_t
{
    return m_frames.bytes_read();
}

auto Pager::bytes_written() const -> std::size_t
{
    return m_frames.bytes_written();
}

auto Pager::page_count() const -> std::size_t
{
    return m_frames.page_count();
}

auto Pager::page_size() const -> std::size_t
{
    return m_frames.page_size();
}

auto Pager::pin_frame(Id pid) -> Status
{
    if (auto s = do_pin_frame(pid); s.is_not_found()) {
        m_info_log->logv("failed to pin frame: %s", s.to_string().data());
        CDB_TRY(m_wal->flush());
        return do_pin_frame(pid);
    } else {
        return s;
    }
}

auto Pager::do_pin_frame(Id pid) -> Status
{
    CDB_EXPECT_FALSE(m_cache.contains(pid));

    if (!m_frames.available()) {
        if (!make_frame_available()) {
            CDB_TRY(m_state->status);
            m_info_log->logv("out of frames: flushing wal");
            CDB_TRY(m_wal->flush());
            return Status::not_found("out of frames");
        }
    }

    std::size_t fid;
    CDB_TRY(m_frames.pin(pid, fid));

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
        target_lsn = kMaxId;
    }

    auto largest = Id::null();
    for (auto itr = m_dirty.begin(); itr != m_dirty.end();) {
        const auto [page_id, record_lsn] = *itr;
        CDB_EXPECT_TRUE(m_cache.contains(page_id));
        auto &entry = m_cache.get(page_id)->value;
        const auto page_lsn = m_frames.get_frame(entry.index).lsn();

        if (largest < page_lsn) {
            largest = page_lsn;
        }

        if (page_lsn > m_wal->flushed_lsn()) {
            // WAL record referencing this page has not been flushed yet.
        } else if (record_lsn <= target_lsn) {
            // Flush the page.
            auto s = m_frames.write_back(entry.index);

            // Advance to the next dirty list entry.
            itr = clean_page(entry);
            CDB_TRY(s);
            continue;
        }
        itr = next(itr);
    }

    // We have flushed the entire cache.
    if (target_lsn == kMaxId) {
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

        if (frame.ref_count != 0) {
            return false;
        }
        if (!entry.token) {
            return true;
        }
        if (m_state->is_running) {
            return frame.lsn() <= m_wal->flushed_lsn();
        }
        return true;
    });

    if (!evicted.has_value()) {
        if (auto s = m_wal->flush(); !s.is_ok()) {
            SET_STATUS(s);
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
        SET_STATUS(s);
        return false;
    }
    return true;
}

auto Pager::allocate(Page *page) -> Status
{
    CDB_TRY(acquire(Id::from_index(m_frames.page_count()), page));
    if (page->id() > m_state->max_page_id) {
        upgrade(*page, 0);
    } else {
        // This page already has its full contents in the WAL. This page must have
        // been vacuumed since the last checkpoint.
        auto itr = m_cache.get(page->id());
        CDB_EXPECT_NE(itr, m_cache.end());
        m_frames.upgrade(itr->value.index, *page);
        itr->value.token = m_dirty.insert(page->id(), Lsn::null());
    }
    ++m_state->max_page_id.value;
    return Status::ok();
}

auto Pager::acquire(Id page_id, Page *page) -> Status
{
    CDB_EXPECT_FALSE(page_id.is_null());

    const auto do_acquire = [&](auto &entry) {
        m_frames.ref(entry.index, *page);

        // Write back pages that are too old. This is so that old WAL segments can be removed.
        if (m_state->is_running && entry.token) {
            const auto should_write = (*entry.token)->record_lsn <= m_state->commit_lsn &&
                                      read_page_lsn(*page) <= m_wal->flushed_lsn();
            if (should_write) {
                auto s = m_frames.write_back(entry.index);

                if (s.is_ok()) {
                    clean_page(entry);
                } else {
                    SET_STATUS(s);
                    return s;
                }
            }
        }
        return Status::ok();
    };

    if (auto itr = m_cache.get(page_id); itr != m_cache.end()) {
        return do_acquire(itr->value);
    }

    CDB_TRY(pin_frame(page_id));

    const auto itr = m_cache.get(page_id);
    CDB_EXPECT_NE(itr, m_cache.end());
    return do_acquire(itr->value);
}

auto Pager::upgrade(Page &page, int important) -> void
{
    CDB_EXPECT_LE(important, static_cast<int>(page.size()));
    const auto lsn = read_page_lsn(page);
    auto itr = m_cache.get(page.id());
    CDB_EXPECT_NE(itr, m_cache.end());

    // The "important" parameter should be used when we don't need to track the before contents of the whole
    // page. For example, when allocating a page from the freelist, we only care about the page LSN stored in
    // the first 8 bytes; the rest is junk.
    std::size_t watch_size;
    if (important < 0) {
        watch_size = page.size();
    } else {
        watch_size = static_cast<std::size_t>(important);
    }

    // Make sure this page is in the dirty list. This is one place where the "record LSN" is set.
    if (!itr->value.token.has_value()) {
        itr->value.token = m_dirty.insert(page.id(), lsn);
    }
    m_frames.upgrade(itr->value.index, page);

    // We only write a full image if the WAL does not already contain one for this page. If the page was modified
    // during this transaction, then we already have one written.
    if (m_state->is_running) {
        if (lsn <= m_state->commit_lsn) {
            const auto image = page.view(0, watch_size);
            auto s = m_wal->log_image(page.id(), image, nullptr);

            if (s.is_ok()) {
                s = m_wal->cleanup(std::min(m_recovery_lsn, m_state->commit_lsn));
            }
            if (!s.is_ok()) {
                SET_STATUS(s);
            }
        }
    }
}

auto Pager::release(Page page) -> void
{
    CDB_EXPECT_GT(m_frames.ref_sum(), 0);
    CDB_EXPECT_TRUE(m_cache.contains(page.id()));
    auto [index, token] = m_cache.get(page.id())->value;

    if (page.is_writable() && m_state->is_running) {
        write_page_lsn(page, m_wal->current_lsn());
        auto s = m_wal->log_delta(
            page.id(), page.view(0), page.deltas(), nullptr);
        if (!s.is_ok()) {
            SET_STATUS(s);
        }
    }
    m_frames.unref(index, std::move(page));
}

auto Pager::truncate(std::size_t page_count) -> Status
{
    CDB_EXPECT_GT(page_count, 0);
    CDB_TRY(m_env->resize_file(m_filename, page_count * m_frames.page_size()));
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
    return flush();
}

auto Pager::save_state(FileHeader &header) -> void
{
    m_frames.save_state(header);
}

auto Pager::load_state(const FileHeader &header) -> void
{
    m_frames.load_state(header);
}

} // namespace calicodb
