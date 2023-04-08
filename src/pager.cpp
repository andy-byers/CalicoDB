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

#define SET_ERROR(s)                   \
    do {                               \
        m_mode = kError;               \
        if (m_state->status.is_ok()) { \
            m_state->status = s;       \
        }                              \
    } while (0)

auto Pager::mode() const -> Mode
{
    return m_mode;
}

auto Pager::purge_entry(CacheEntry &victim) -> void
{
    if (is_dirty(victim)) {
        clean_page(victim);
    }
    m_frames.unpin(victim);
    // Invalidates storage for "victim".
    m_cache.erase(victim.page_id);
}

auto Pager::populate_entry(CacheEntry &out) -> Status
{
    char *page = nullptr;
    Status s;

    if (m_state->use_wal) {
        // Try to read the page from the WAL. Nulls out "page" if it cannot find the
        // page.
        page = out.page;
        s = m_wal->read(out.page_id, page);
    }

    if (s.is_ok() && page == nullptr) {
        // Read the page from the DB file.
        s = read_page_from_file(out);
    }

    if (!s.is_ok()) {
        m_frames.unpin(out);
        if (m_mode == kDirty) {
            SET_ERROR(s);
        }
    }
    return s;
}

auto Pager::cache_entry(Id page_id, CacheEntry *&out) -> Status
{
    Status s;
    out = m_cache.get(page_id);
    if (out == nullptr) {
        out = m_cache.alloc(page_id);
        m_frames.pin(*out);
        return populate_entry(*out);
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

auto Pager::open(const Parameters &param, Pager *&out) -> Status
{
    CALICODB_EXPECT_TRUE(is_power_of_two(param.page_size));
    CALICODB_EXPECT_GE(param.page_size, kMinPageSize);
    CALICODB_EXPECT_LE(param.page_size, kMaxPageSize);
    CALICODB_EXPECT_GE(param.frame_count, kMinFrameCount);
    CALICODB_EXPECT_LE(param.page_size * param.frame_count, kMaxCacheSize);
    const auto exists = param.env->file_exists(param.filename);

    File *file;
    CALICODB_TRY(param.env->new_file(param.filename, file));

    // Allocate the frames, i.e. where pages from disk are stored in memory. Aligned
    // to the page size, so it could potentially be used for direct I/O.
    AlignedBuffer buffer(
        param.page_size * param.frame_count,
        param.page_size);

    out = new Pager(param, *file, std::move(buffer));

    Status s;
    if (exists) {
        s = out->read_page_from_file(out->m_root);
    } else {
        // Write the root page, initially blank.
        s = out->write_page_to_file(out->m_root);
        ++out->m_page_count;
    }
    if (!s.is_ok()) {
        delete out;
        out = nullptr;
    }
    return s;
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
    m_root.page_id = Id::root();
    m_frames.pin(m_root);
}

Pager::~Pager()
{
    m_frames.unpin(m_root);
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

auto Pager::is_dirty(const CacheEntry &entry) const -> bool
{
    return entry.next || entry.prev || &entry == m_dirty;
}

auto Pager::dirty_page(CacheEntry &entry) -> void
{
    CALICODB_EXPECT_FALSE(is_dirty(entry));
    if (m_dirty) {
        CALICODB_EXPECT_FALSE(m_dirty->prev);
        m_dirty->prev = &entry;
    }
    entry.prev = nullptr;
    entry.next = m_dirty;
    m_dirty = &entry;
}

auto Pager::clean_page(CacheEntry &entry) -> CacheEntry *
{
    CALICODB_EXPECT_TRUE(m_dirty);
    CALICODB_EXPECT_FALSE(m_dirty->prev);
    CALICODB_EXPECT_TRUE(is_dirty(entry));

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
    entry.prev = nullptr;
    entry.next = nullptr;
    return next;
}

auto Pager::begin_txn() -> bool
{
    CALICODB_EXPECT_NE(m_mode, kError);
    if (m_mode == kOpen) {
        m_mode = kWrite;
        return true;
    }
    return false;
}

auto Pager::commit_txn() -> Status
{
    CALICODB_EXPECT_TRUE(m_state->use_wal);
    CALICODB_EXPECT_GE(m_mode, kWrite);

    // Report prior errors again.
    CALICODB_TRY(m_state->status);

    if (m_mode == kWrite) {
        // No work done in this transaction.
        CALICODB_EXPECT_EQ(m_dirty, nullptr);
        m_mode = kOpen;
    }

    if (m_mode == kDirty) {
        if (m_dirty == nullptr) {
            // Ensure that there is always a WAL frame to store the DB size.
            m_dirty = &m_root;
        }
        auto *p = m_dirty;
        for (; p; p = p->next) {
            if (p->page_id.value > m_page_count) {
                // This page is past the current end of the file due to a vacuum operation
                // decreasing the page count. Just remove the page from the dirty list. It 
                // wouldn't be transferred back to the DB on checkpoint anyway since it is 
                // out of bounds.
                p = clean_page(*p);
            }
        }
        p = m_dirty;
        CALICODB_EXPECT_NE(p, nullptr);
        m_state->batch_size = 0;
        m_dirty = nullptr;
        m_mode = kOpen;

        // The DB page count is specified here. This indicates that the writes are part of
        // a commit.
        auto s = m_wal->write(p, m_page_count);
        if (!s.is_ok()) {
            SET_ERROR(s);
        }
        return s;
    }
    return Status::ok();
}

auto Pager::rollback_txn() -> Status
{
    CALICODB_EXPECT_TRUE(m_state->use_wal);
    if (m_mode <= kWrite) {
        m_mode = kOpen;
        return Status::ok();
    }
    if (m_in_ckpt) {
        CALICODB_EXPECT_EQ(m_mode, kError);
        return checkpoint();
    }
    auto s = m_wal->abort();
    if (s.is_ok()) {
        // Refresh the in-memory DB root page.
        s = populate_entry(m_root);
    }
    if (s.is_ok()) {
        purge_cache();
        m_state->status = Status::ok();
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
        purge_entry(*victim);
    }
}

auto Pager::checkpoint() -> Status
{
    CALICODB_EXPECT_TRUE(
        m_mode == kOpen || // Normal checkpoint, right after a commit
        (m_mode == kError && m_in_ckpt)); // Attempt to fix a failed checkpoint

    m_in_ckpt = true;
    auto s = wal_checkpoint();
    if (s.is_ok()) {
        m_in_ckpt = false;
        m_mode = kOpen;
    } else {
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
    CALICODB_TRY(m_file->sync());
    return read_page_from_file(m_root);
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
            purge_entry(*out_of_range);
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

        if (is_dirty(*victim)) {
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
                // This is an error, regardless of what mode the pager is in.
                SET_ERROR(s);
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

    // Ensure that the next call to "m_frames.pin()" has an available slot in
    // the frame buffer.
    CALICODB_TRY(ensure_available_frame());

    CacheEntry *entry;
    if (page_id.value > m_page_count) {
        // This is a new page from the end of the file.
        entry = m_cache.alloc(page_id);
        m_frames.pin(*entry);
        std::memset(entry->page, 0, m_frames.page_size());
    } else if (!page_id.is_root()) {
        // Read a page from either the WAL or the DB.
        CALICODB_TRY(cache_entry(page_id, entry));
    } else {
        entry = &m_root;
    }
    m_frames.ref(*entry, page);
    return Status::ok();
}

auto Pager::acquire_root() -> Page
{
    Page root;
    m_frames.ref(m_root, root);
    return root;
}

auto Pager::upgrade(Page &page) -> void
{
    CALICODB_EXPECT_TRUE(
        !m_state->use_wal || // In initialization routine
        m_mode >= kWrite); // Transaction has started

    if (!is_dirty(*page.entry())) {
        dirty_page(*page.entry());
        if (m_mode == kWrite) {
            m_mode = kDirty;
        }
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

auto Pager::TEST_validate() const -> void
{
#ifndef NDEBUG
    if (m_mode <= kWrite) {
        CALICODB_EXPECT_EQ(m_dirty, nullptr);
    } else {
        if (m_mode == kDirty) {
            CALICODB_EXPECT_NE(m_dirty, nullptr);
        }
        auto *p = m_dirty;
        while (p) {
            CALICODB_EXPECT_TRUE(is_dirty(*p));
            p = p->next;
        }
    }
#endif // NDEBUG
}

#undef SET_ERROR

} // namespace calicodb
