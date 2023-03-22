// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "pager.h"
#include "db_impl.h"
#include "header.h"
#include "logging.h"
#include "page.h"
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
    CALICODB_EXPECT_TRUE(is_power_of_two(param.page_size));
    CALICODB_EXPECT_GE(param.page_size, kMinPageSize);
    CALICODB_EXPECT_LE(param.page_size, kMaxPageSize);
    CALICODB_EXPECT_GE(param.frame_count, kMinFrameCount);
    CALICODB_EXPECT_LE(param.page_size * param.frame_count, kMaxCacheSize);

    Editor *file;
    CALICODB_TRY(param.env->new_editor(param.filename, file));

    // Allocate the frames, i.e. where pages from disk are stored in memory. Aligned to the page size, so it could
    // potentially be used for direct I/O.
    AlignedBuffer buffer {
        param.page_size * param.frame_count,
        param.page_size};

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
    CALICODB_EXPECT_NE(m_wal, nullptr);
    CALICODB_EXPECT_NE(m_state, nullptr);
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
    return m_page_count;
}

auto Pager::page_size() const -> std::size_t
{
    return m_frames.page_size();
}

auto Pager::clean_page(CacheEntry &entry) -> DirtyTable::Iterator
{
    auto token = *entry.token;
    // Reset the dirty list reference.
    entry.token.reset();
    return m_dirty.remove(token);
}

auto Pager::checkpoint() -> Status
{
    Lsn obsolete_lsn;
    CALICODB_TRY(m_wal->find_obsolete_lsn(obsolete_lsn));
    if (!obsolete_lsn.is_null()) {
        // Flush dirty pages that are on old WAL segments.
        CALICODB_TRY(flush(obsolete_lsn));
    }
    CALICODB_TRY(m_frames.sync());

    // Oldest LSN still needed for a cached page. fsync() was just called, so everything
    // before this should be on disk.
    m_recovery_lsn = m_dirty.recovery_lsn();
    return m_wal->cleanup(m_recovery_lsn);
}

auto Pager::flush(Lsn target_lsn) -> Status
{
    // An LSN of NULL causes all pages to be flushed.
    if (target_lsn.is_null()) {
        target_lsn = kMaxId;
    }

    for (auto itr = m_dirty.begin(); itr != m_dirty.end();) {
        const auto [record_lsn, page_id] = *itr;
        auto *entry = m_cache.query(page_id);
        CALICODB_EXPECT_NE(entry, nullptr);
        const auto &frame = m_frames.get_frame(entry->index);
        const auto page_lsn = read_page_lsn(page_id, frame.data);

        if (record_lsn > target_lsn) {
            break;
        }
        if (page_lsn <= m_wal->flushed_lsn()) {
            CALICODB_TRY(m_frames.write_back(entry->index));
            itr = clean_page(*entry);
        } else {
            itr = next(itr);
        }
    }
    return Status::ok();
}

auto Pager::recovery_lsn() const -> Lsn
{
    return m_recovery_lsn;
}

auto Pager::make_frame_available() -> void
{
    auto evicted = m_cache.evict();
    CALICODB_EXPECT_TRUE(evicted.has_value());

    if (evicted->token) {
        Status s;
        const auto &frame = m_frames.get_frame(evicted->index);
        if (m_state->is_running) {
            const auto page_lsn = read_page_lsn(frame.page_id, frame.data);
            if (page_lsn > m_wal->flushed_lsn()) {
                // Only flush the tail buffer if necessary. Otherwise, just fsync() the WAL file.
                s = m_wal->synchronize(page_lsn > m_wal->written_lsn());
            }
        }
        if (s.is_ok()) {
            // NOTE: We don't update the record LSN field because we are getting rid of this page.
            s = m_frames.write_back(evicted->index);
        }
        if (!s.is_ok()) {
            SET_STATUS(s);
        }
        clean_page(*evicted);
    }
    m_frames.unpin(*evicted);
}

auto Pager::allocate(Page &page) -> Status
{
    CALICODB_TRY(acquire(Id::from_index(m_page_count), page));
    upgrade(page, 0);
    ++m_page_count;
    return Status::ok();
}

auto Pager::acquire(Id page_id, Page &page) -> Status
{
    CALICODB_EXPECT_FALSE(page_id.is_null());
    CALICODB_TRY(m_state->status);

    if (!m_frames.available()) {
        make_frame_available();
        CALICODB_TRY(m_state->status);
    }
    auto *entry = m_cache.get(page_id);
    if (entry == nullptr) {
        CacheEntry temp;
        CALICODB_TRY(m_frames.pin(page_id, temp));
        entry = m_cache.put(temp);
    }
    return m_frames.ref(*entry, page);
}

auto Pager::upgrade(Page &page, int important) -> void
{
    CALICODB_EXPECT_LE(important, static_cast<int>(page.size()));
    const auto page_lsn = read_page_lsn(page);

    // The "important" parameter should be used when we don't need to track the before contents of
    // the whole page. For example, when allocating a page from the freelist, we only care about
    // the page LSN stored in the first 8 bytes; the rest is junk.
    auto watch_size = page.size();
    if (important >= 0) {
        watch_size = static_cast<std::size_t>(important);
    }

    // Make sure this page is in the dirty list. This is one place where the "record LSN" is set.
    if (!page.entry()->token.has_value()) {
        page.entry()->token = m_dirty.insert(page.id(), page_lsn);
    }
    m_frames.upgrade(page);

    // Skip writing an image record if there is definitely one for this page during this transaction.
    if (m_state->is_running && page_lsn <= m_state->commit_lsn) {
        const auto image = page.view(0, watch_size);
        const auto s = m_wal->log_image(page.id(), image, nullptr);
        if (!s.is_ok()) {
            SET_STATUS(s);
        }
    }
}

auto Pager::release(Page page, bool discard) -> void
{
    CALICODB_EXPECT_NE(page.entry()->refcount, 0);
    m_frames.unref(*page.entry());

    if (m_state->is_running && page.is_writable()) {
        write_page_lsn(page, m_wal->current_lsn());
        auto deltas = m_frames.collect_deltas(page);
//        deltas = page.deltas();//{PageDelta {0, page.size()}};
        if (!discard) {
            auto s = m_wal->log_delta(
                page.id(), page.view(0), deltas, nullptr);
            if (!s.is_ok()) {
                SET_STATUS(s);
            }
        }
    }
}

auto Pager::truncate(std::size_t page_count) -> Status
{
    CALICODB_EXPECT_GT(page_count, 0);
    CALICODB_TRY(m_env->resize_file(m_filename, page_count * m_frames.page_size()));

    // Discard out-of-range cached pages.
    for (Id id {page_count + 1}; id.value <= m_page_count; ++id.value) {
        auto *entry = m_cache.query(id);
        if (entry != nullptr) {
            if (entry->token.has_value()) {
                m_dirty.remove(*entry->token);
            }
            m_frames.unpin(*entry);
            m_cache.erase(id);
        }
    }
    m_page_count = page_count;
    return Status::ok();
}

auto Pager::load_state(const FileHeader &header) -> void
{
    m_page_count = header.page_count;
}

} // namespace calicodb
