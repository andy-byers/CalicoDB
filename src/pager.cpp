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

auto Pager::mode() const -> Mode
{
    return m_mode;
}

auto Pager::purge_page(CacheEntry &victim) -> void
{
    if (victim.is_dirty) {
        clean_page(victim);
    }
    m_frames.unpin(victim);
    // Invalidates storage for "victim".
    m_cache.erase(victim.page_id);
}

auto Pager::fetch_page(Id page_id, CacheEntry *&out) -> Status
{
    Status s;
    out = m_cache.get(page_id);
    if (out == nullptr) {
        out = m_cache.alloc(page_id);
        m_frames.pin(*out);
        char *page = nullptr;

        if (m_state->use_wal) {
            // Try to read the page from the WAL. Nulls out "page" if it cannot find the
            // page.
            page = out->page;
            s = m_wal->read(page_id, page);
        }

        if (s.is_ok() && page == nullptr) {
            // Read the page from the DB file.
            s = read_page_from_file(*out);
        }

        if (!s.is_ok()) {
            m_frames.unpin(*out);
            m_cache.erase(page_id);
            out = nullptr;
            if (m_mode == kDirty) {
                m_mode = kError;
            }
        }
    }
    return s;
}

auto Pager::read_page_from_file(CacheEntry &entry) const -> Status
{
    if (entry.page_id.as_index() < m_page_count) {
        const auto offset = entry.page_id.as_index() * m_frames.page_size();
        CALICODB_TRY(m_file->read_exact(offset, m_frames.page_size(), entry.page));
        m_bytes_read += m_frames.page_size();
    } else {
        std::memset(entry.page, 0, m_frames.page_size());
    }
    return Status::ok();
}

auto Pager::write_page_to_file(const CacheEntry &entry) const -> Status
{
    const Slice data(entry.page, m_frames.page_size());
    m_bytes_written += data.size();
    return m_file->write(entry.page_id.as_index() * data.size(), data);
}

auto Pager::open(const Parameters &param, Pager **out) -> Status
{
    CALICODB_EXPECT_TRUE(is_power_of_two(param.page_size));
    CALICODB_EXPECT_GE(param.page_size, kMinPageSize);
    CALICODB_EXPECT_LE(param.page_size, kMaxPageSize);
    CALICODB_EXPECT_GE(param.frame_count, kMinFrameCount);
    CALICODB_EXPECT_LE(param.page_size * param.frame_count, kMaxCacheSize);

    File *file;
    CALICODB_TRY(param.env->new_file(param.filename, file));

    // Allocate the frames, i.e. where pages from disk are stored in memory. Aligned
    // to the page size, so it couldnpotentially be used for direct I/O.
    AlignedBuffer buffer(
        param.page_size * param.frame_count,
        param.page_size);

    *out = new Pager(param, *file, std::move(buffer));
    return Status::ok();
}

Pager::Pager(const Parameters &param, File &file, AlignedBuffer buffer)
    : m_filename(param.filename),
      m_frames(std::move(buffer), param.page_size, param.frame_count),
      m_log(param.log),
      m_file(&file),
      m_env(param.env),
      m_wal(param.wal),
      m_state(param.state)
{
    CALICODB_EXPECT_NE(m_state, nullptr);
}

Pager::~Pager()
{
    delete m_file;
}

auto Pager::bytes_read() const -> std::size_t
{
    return m_bytes_read;
}

auto Pager::bytes_written() const -> std::size_t
{
    return m_bytes_written;
}

auto Pager::page_count() const -> std::size_t
{
    return m_page_count;
}

auto Pager::page_size() const -> std::size_t
{
    return m_frames.page_size();
}

auto Pager::dirty_page(CacheEntry &entry) -> void
{
    CALICODB_EXPECT_FALSE(entry.is_dirty);
    if (m_dirty) {
        m_dirty->prev = &entry;
    }
    entry.is_dirty = true;
    entry.prev = nullptr;
    entry.next = m_dirty;
    m_dirty = &entry;
}

auto Pager::clean_page(CacheEntry &entry) -> CacheEntry *
{
    entry.is_dirty = false;
    if (entry.prev) {
        entry.prev->next = entry.next;
    } else {
        CALICODB_EXPECT_EQ(&entry, m_dirty);
        m_dirty = entry.next;
    }
    auto *next = entry.next;
    if (next) {
        next->prev = entry.prev;
    }
    return next;
}

auto Pager::begin_txn() -> Status
{
    m_mode = kWrite;
    return Status::ok();
}

auto Pager::commit_txn() -> Status
{
    CALICODB_EXPECT_TRUE(m_state->use_wal);
    if (m_dirty == nullptr) {
        m_mode = kOpen;
        // TODO: It's possible that a large query forced eviction and write back of dirty pages, but we still need a commit. Just write the root page to the WAL.
        return Status::ok();
    }
    auto *p = m_dirty;
    for (; p; p = p->next) {
        if (p->page_id.value > m_page_count) {
            // This page is past the current end of the file due to a vacuum operation
            // decreasing the database page count. Just remove the page from the dirty
            // list. It wouldn't be transferred back to the DB on checkpoint anyway
            // since it is out of bounds.
            p = clean_page(*p);
        } else {
            p->is_dirty = false;
        }
    }
    p = m_dirty;
    CALICODB_EXPECT_NE(p, nullptr);
    m_dirty = nullptr;

    m_state->batch_size = 0;
    // The DB page count is specified here. This indicates that the writes are part of
    // a commit.
    auto s = m_wal->write(p, m_page_count);
    if (!s.is_ok()) {
        m_mode = kError;
    }
    return s;
}

auto Pager::rollback_txn() -> Status
{
    CALICODB_EXPECT_TRUE(m_state->use_wal);
    if (m_dirty == nullptr) {
        return Status::ok();
    }

    auto s = m_wal->abort();
    if (s.is_ok()) {
        purge_cache();
        m_mode = kOpen;
    } else {
        m_mode = kError;
    }
    return s;
}

auto Pager::purge_cache() -> void
{
    // Note that this will leave referenced pages in the cache.
    CacheEntry *victim;
    while ((victim = m_cache.next_victim())) {
        CALICODB_EXPECT_NE(victim, nullptr);
        purge_page(*victim);
    }
}

auto Pager::checkpoint() -> Status
{
    CALICODB_EXPECT_EQ(m_mode, kOpen);
    auto s = wal_checkpoint();
    if (!s.is_ok()) {
        m_mode = kError;
    }
    return s;
}

auto Pager::wal_checkpoint() -> Status
{
    // A checkpoint must immediately follow a commit, so the cache should be clean.
    CALICODB_EXPECT_EQ(m_dirty, nullptr);
    std::size_t dbsize;

    // Transfer the WAL contents back to the DB. Note that this call will sync the WAL
    // file before it starts transferring any data back.
    CALICODB_TRY(m_wal->checkpoint(*m_file, &dbsize));

    if (dbsize && dbsize != m_page_count) {
        set_page_count(dbsize);
        CALICODB_TRY(m_env->resize_file(m_filename, dbsize * m_frames.page_size()));
    }
    return m_file->sync();
}

auto Pager::flush_to_disk() -> Status
{
    for (auto *p = m_dirty; p; p = clean_page(*p)) {
        CALICODB_TRY(write_page_to_file(*p));
    }
    return m_file->sync();
}

auto Pager::set_page_count(std::size_t page_count) -> void
{
    for (auto i = page_count; i < m_page_count; ++i) {
        if (auto *out_of_range = m_cache.query(Id::from_index(i))) {
            purge_page(*out_of_range);
        }
    }
    m_page_count = page_count;
}

auto Pager::ensure_available_frame() -> Status
{
    Status s;
    if (!m_frames.available()) {
        // There are no available frames, so the cache must be full. "next_victim()" will not find
        // an entry to evict if all pages are referenced, which should never happen.
        auto *victim = m_cache.next_victim();
        CALICODB_EXPECT_NE(victim, nullptr);

        if (victim->is_dirty) {
            clean_page(*victim);

            if (m_state->use_wal) {
                // Write just this page to the WAL. DB page count is 0 here because this write
                // is not part of a commit.
                s = m_wal->write(&*victim, 0);
            } else {
                // WAL is not enabled, meaning this code is called from either recovery, checkpoint,
                // or initialization.
                s = write_page_to_file(*victim);
            }
            if (!s.is_ok()) {
                m_mode = kError;
            }
        }
        m_frames.unpin(*victim);
        m_cache.erase(victim->page_id);
    }
    return s;
}

auto Pager::allocate(Page &page) -> Status
{
    static constexpr std::size_t kMaxPageCount = 0xFFFFFFFF - 1;
    if (m_page_count == kMaxPageCount) {
        return Status::not_supported("reached the maximum database size");
    }
    auto s = acquire(Id::from_index(m_page_count), page);
    if (s.is_ok()) {
        ++m_page_count;
        upgrade(page);
    }
    return s;
}

auto Pager::acquire(Id page_id, Page &page) -> Status
{
    CALICODB_EXPECT_FALSE(page_id.is_null());
    CALICODB_TRY(ensure_available_frame());

    CacheEntry *entry;
    if (page_id.value > m_page_count) {
        // This is a new page from the end of the file.
        entry = m_cache.alloc(page_id);
        m_frames.pin(*entry);
        std::memset(entry->page, 0, m_frames.page_size());
    } else {
        // Read a page from either the WAL or the DB.
        CALICODB_TRY(fetch_page(page_id, entry));
    }
    m_frames.ref(*entry, page);
    return Status::ok();
}

auto Pager::upgrade(Page &page) -> void
{
    if (!page.entry()->is_dirty) {
        dirty_page(*page.entry());
    }
    m_frames.upgrade(page);
}

auto Pager::release(Page page) -> void
{
    CALICODB_EXPECT_NE(page.entry()->refcount, 0);
    m_frames.unref(*page.entry());
}

auto Pager::load_state(const FileHeader &header) -> void
{
    m_page_count = header.page_count;
}

} // namespace calicodb
