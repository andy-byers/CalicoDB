// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "pager.h"
#include "alloc.h"
#include "calicodb/env.h"
#include "header.h"
#include "logging.h"
#include "node.h"
#include "stat.h"
#include "temp.h"

namespace calicodb
{

auto Pager::set_page_size(uint32_t value) -> Status
{
    CALICODB_EXPECT_LT(m_mode, kError);
    CALICODB_EXPECT_NE(m_page_size, value);
    CALICODB_EXPECT_EQ(m_bufmgr.refsum(), 0);
    CALICODB_EXPECT_TRUE(m_dirtylist.is_empty());

    // In-memory databases only have the page size set once, on startup. Every other time this
    // method is called, the header page size must match m_page_size.
    CALICODB_EXPECT_TRUE(m_persistent || m_page_size == 0);

    // Reallocate buffers and recompute values that depend on the page size.
    if (m_scratch.realloc(value * kScratchBufferPages)) {
        return Status::no_memory();
    }
    if (m_bufmgr.reallocate(value)) {
        return Status::no_memory();
    }
    m_page_size = value;
    return Status::ok();
}

auto Pager::purge_page(PageRef &victim) -> void
{
    if (victim.get_flag(PageRef::kDirty)) {
        m_dirtylist.remove(victim);
    }
    m_bufmgr.erase(victim);
}

auto Pager::read_page(PageRef &page_out, size_t *size_out) -> Status
{
    // Try to read the page from the WAL.
    auto *page = page_out.data;
    auto s = m_wal->read(page_out.page_id, m_page_size, page);
    if (s.is_ok()) {
        if (page == nullptr) {
            // No error, but the page could not be located in the WAL. Read the page
            // from the DB file instead.
            s = read_page_from_file(page_out, size_out);
        } else if (size_out) {
            *size_out = m_page_size;
        }
    }

    if (!s.is_ok()) {
        m_bufmgr.erase(page_out);
        if (m_mode > kRead) {
            set_status(s);
        }
    }
    return s;
}

auto Pager::read_page_from_file(PageRef &ref, size_t *size_out) const -> Status
{
    Slice slice;
    const auto offset = ref.page_id.as_index() * m_page_size;
    auto s = m_file->read(offset, m_page_size, ref.data, &slice);
    if (s.is_ok()) {
        m_stat->counters[Stat::kReadDB] += slice.size();
        std::memset(ref.data + slice.size(), 0, m_page_size - slice.size());
        if (size_out) {
            *size_out = slice.size();
        }
    }
    return s;
}

auto Pager::open_wal() -> Status
{
    CALICODB_EXPECT_EQ(m_wal, nullptr);
    const Wal::Parameters wal_param = {
        m_wal_name,
        m_env,
        m_file,
        m_log,
        m_stat,
        m_busy,
        m_sync_mode,
        m_lock_mode,
    };
    if (m_persistent) {
        return Wal::open(wal_param, m_wal);
    }
    m_wal = new_temp_wal(wal_param);
    if (m_wal == nullptr) {
        return Status::no_memory();
    }
    return Status::ok();
}

auto Pager::close_wal() -> Status
{
    CALICODB_EXPECT_EQ(m_mode, kOpen);
    Status s;
    if (!m_wal && m_env->file_exists(m_wal_name)) {
        s = open_wal();
    }
    if (s.is_ok() && m_wal) {
        // This connection already has a shared lock on the DB file. Attempt to upgrade to an
        // exclusive lock, which, if successful would indicate that this is the only connection.
        // If this connection is using the Options::kLockExclusive lock mode, this call is a
        // NOOP, since the file is already locked in this mode. Released in Pager::close().
        s = m_file->file_lock(kFileExclusive);
        if (s.is_ok()) {
            s = m_wal->close(m_scratch.ptr(), m_page_size); // TODO: Page size may not be correct if a transaction was never started.
        } else if (s.is_busy()) {
            s = Status::ok();
        }
    }
    Alloc::delete_object(m_wal);
    m_wal = nullptr;
    return s;
}

auto Pager::open(const Parameters &param, Pager *&pager_out) -> Status
{
    Status s;
    pager_out = Alloc::new_object<Pager>(param);
    if (pager_out) {
        s = pager_out->set_page_size(param.page_size);
    } else {
        s = Status::no_memory();
    }
    if (!s.is_ok()) {
        Alloc::delete_object(pager_out);
        pager_out = nullptr;
    }
    return s;
}

Pager::Pager(const Parameters &param)
    : m_bufmgr((param.cache_size + param.page_size - 1) / param.page_size, *param.stat),
      m_status(param.status),
      m_log(param.log),
      m_env(param.env),
      m_file(param.db_file),
      m_stat(param.stat),
      m_busy(param.busy),
      m_lock_mode(param.lock_mode),
      m_sync_mode(param.sync_mode),
      m_persistent(param.persistent),
      m_db_name(param.db_name),
      m_wal_name(param.wal_name)
{
    CALICODB_EXPECT_NE(m_file, nullptr);
    CALICODB_EXPECT_NE(m_status, nullptr);
    CALICODB_EXPECT_NE(m_stat, nullptr);
    CALICODB_EXPECT_NE(m_db_name, nullptr);
    CALICODB_EXPECT_NE(m_wal_name, nullptr);
}

Pager::~Pager()
{
    CALICODB_EXPECT_EQ(m_mode, kOpen);
    CALICODB_EXPECT_TRUE(m_status->is_ok());
    Alloc::delete_object(m_wal);
}

auto Pager::close() -> void
{
    finish();
    auto s = close_wal();
    // Regardless of lock mode, this is where the database file lock is released. The
    // database file should not be accessed after this point.
    m_file->file_unlock();

    if (!s.is_ok()) {
        log(m_log, "failed to shutdown pager due to %s", s.message());
    }
}

auto Pager::start_reader() -> Status
{
    CALICODB_EXPECT_NE(kError, m_mode);
    CALICODB_EXPECT_TRUE(assert_state());

    if (m_mode != kOpen) {
        return *m_status;
    }
    if (m_wal) {
        m_wal->finish_reader();
    } else {
        auto s = open_wal();
        if (!s.is_ok()) {
            return s;
        }
    }
    bool changed;
    auto s = busy_wait(m_busy, [this, &changed] {
        return m_wal->start_reader(changed);
    });
    if (s.is_ok()) {
        if (changed) {
            purge_pages(true);
        }
        if (m_refresh) {
            s = refresh_state();
        }
        if (s.is_ok()) {
            m_mode = kRead;
        }
    }
    if (!s.is_ok()) {
        finish();
    }
    return s;
}

auto Pager::start_writer() -> Status
{
    CALICODB_EXPECT_NE(m_mode, kOpen);
    CALICODB_EXPECT_NE(m_mode, kError);
    CALICODB_EXPECT_TRUE(assert_state());

    Status s;
    if (m_mode == kRead) {
        s = m_wal->start_writer();
        if (s.is_ok()) {
            m_mode = kWrite;
        }
    }
    return s;
}

auto Pager::commit() -> Status
{
    CALICODB_EXPECT_NE(kOpen, m_mode);
    CALICODB_EXPECT_TRUE(assert_state());

    // Report prior errors again.
    auto s = *m_status;
    if (!s.is_ok()) {
        return s;
    }

    if (m_mode == kDirty) {
        // Update the page count if necessary.
        auto &root = get_root();
        if (m_page_count != m_saved_page_count) {
            mark_dirty(root);
            FileHdr::put_page_count(root.data, m_page_count);
        }
        if (m_dirtylist.is_empty()) {
            // Ensure that there is always a WAL frame to store the DB size.
            m_dirtylist.add(*m_bufmgr.root());
        }
        // Write all dirty pages to the WAL.
        s = flush_dirty_pages();
        if (s.is_ok()) {
            m_saved_page_count = m_page_count;
            m_mode = kWrite;
        } else {
            set_status(s);
        }
    }
    return s;
}

auto Pager::move_page(PageRef &page, Id destination) -> void
{
    // Caller must have called Pager::release(<page at `destination`>, Pager::kDiscard).
    CALICODB_EXPECT_EQ(m_bufmgr.query(destination), nullptr);
    CALICODB_EXPECT_EQ(page.refs, 1);
    m_bufmgr.erase(page);
    page.page_id = destination;
    if (page.get_flag(PageRef::kDirty)) {
        m_bufmgr.register_page(page);
    } else {
        mark_dirty(page);
    }
}

auto Pager::undo_callback(void *arg, Id id) -> void
{
    PageRef *ref;
    auto *pager = static_cast<Pager *>(arg);
    if (!id.is_root() && (ref = pager->m_bufmgr.query(id))) {
        pager->purge_page(*ref);
    }
}

auto Pager::finish() -> void
{
    CALICODB_EXPECT_TRUE(assert_state());

    if (m_mode >= kDirty) {
        if (m_mode == kDirty) {
            // Get rid of obsolete cached pages that aren't dirty anymore.
            m_wal->rollback(undo_callback, this);
        }
        m_wal->finish_writer();
        // Get rid of dirty pages, or all cached pages if there was a fault.
        purge_pages(m_mode == kError);
        m_page_count = m_saved_page_count;
    }
    if (m_mode >= kRead) {
        m_wal->finish_reader();
        m_bufmgr.shrink_to_fit();
    }
    *m_status = Status::ok();
    m_mode = kOpen;
}

auto Pager::purge_pages(bool purge_all) -> void
{
    m_refresh = true;

    for (auto *dirty = m_dirtylist.begin(); dirty != m_dirtylist.end();) {
        auto *save = dirty->get_page_ref();
        dirty = dirty->next_entry;
        purge_page(*save);
    }
    CALICODB_EXPECT_TRUE(m_dirtylist.is_empty());

    if (purge_all) {
        m_bufmgr.purge();
    }
}

auto Pager::checkpoint(bool reset) -> Status
{
    CALICODB_EXPECT_EQ(m_mode, kOpen);
    CALICODB_EXPECT_TRUE(assert_state());
    if (m_wal == nullptr) {
        // Ensure that the WAL and WAL index have been created.
        auto s = start_reader();
        if (!s.is_ok()) {
            return s;
        }
        finish();
    }
    return m_wal->checkpoint(reset, m_scratch.ptr(), m_page_size);
}

auto Pager::auto_checkpoint(size_t frame_limit) -> Status
{
    CALICODB_EXPECT_GT(frame_limit, 0);
    if (m_wal && frame_limit < m_wal->last_frame_count()) {
        return checkpoint(false);
    }
    return Status::ok();
}

auto Pager::flush_dirty_pages() -> Status
{
    auto *dirty = m_dirtylist.begin();
    while (dirty != m_dirtylist.end()) {
        auto *page = dirty->get_page_ref();
        CALICODB_EXPECT_TRUE(page->get_flag(PageRef::kDirty));
        if (page->page_id.value > m_page_count) {
            // This page is past the current end of the file due to a vacuum operation
            // decreasing the page count. Just remove the page from the dirty list. It
            // wouldn't be transferred back to the DB on checkpoint anyway since it is
            // out of bounds.
            dirty = m_dirtylist.remove(*page);
        } else {
            page->clear_flag(PageRef::kDirty);
            dirty = dirty->next_entry;
        }
    }
    // These pages are no longer considered dirty. If the call to Wal::write() fails,
    // this connection must purge the whole cache.
    dirty = m_dirtylist.sort();
    CALICODB_EXPECT_NE(dirty, nullptr);

    return m_wal->write(dirty->get_page_ref(), m_page_size, m_page_count);
}

auto Pager::set_page_count(uint32_t page_count) -> void
{
    CALICODB_EXPECT_GE(m_mode, kWrite);
    for (auto i = page_count; i < m_page_count; ++i) {
        if (auto *out_of_range = m_bufmgr.query(Id::from_index(i))) {
            purge_page(*out_of_range);
        }
    }
    m_page_count = page_count;
}

auto Pager::ensure_available_buffer() -> Status
{
    PageRef *victim;
    if (!(victim = m_bufmgr.next_victim()) &&
        !(victim = m_bufmgr.allocate(m_page_size))) {
        return Status::no_memory();
    }

    Status s;
    if (victim->get_flag(PageRef::kDirty)) {
        CALICODB_EXPECT_GE(m_mode, kDirty);
        // Clear the transient list pointer, since we are writing just this page to the WAL.
        // The transient list is not valid unless Dirtylist::sort() was called.
        victim->dirty_hdr.dirty = nullptr;

        // DB page count is 0 here because this write is not part of a commit.
        s = m_wal->write(victim, m_page_size, 0);
        if (s.is_ok()) {
            m_dirtylist.remove(*victim);
        } else {
            set_status(s);
            return s;
        }
    }

    // Erase this page from the buffer manager's lookup table. It will still be returned the
    // next time m_bufmgr.next_victim() is called, it just can't be found using its page ID
    // anymore. This is a NOOP if the page reference was just allocated.
    if (victim->get_flag(PageRef::kCached)) {
        m_bufmgr.erase(*victim);
    }
    return s;
}

auto Pager::allocate(PageRef *&page_out) -> Status
{
    // Allocation of the root page is handled in initialize_root().
    CALICODB_EXPECT_GT(m_page_count, 0);
    CALICODB_EXPECT_GE(m_mode, kWrite);
    page_out = nullptr;

    static constexpr uint32_t kMaxPageCount = 0xFF'FF'FF'FF;
    if (m_page_count == kMaxPageCount) {
        return Status::not_supported("reached the maximum allowed database size");
    }

    const auto page_id = Id::from_index(m_page_count);
    auto s = get_unused_page(page_out);
    if (s.is_ok()) {
        page_out->page_id = page_id;
        m_bufmgr.register_page(*page_out);
        m_page_count = page_id.value;
        // Callers of this routine will always modify `page_out`. Mark it dirty here for
        // convenience. Note that it might already be dirty, if it is a freelist trunk
        // page that has been modified recently.
        mark_dirty(*page_out);
    }
    return s;
}

auto Pager::acquire(Id page_id, PageRef *&page_out) -> Status
{
    CALICODB_EXPECT_GE(m_mode, kRead);
    Status s;

    if (page_id.is_null() || page_id.value > m_page_count) {
        page_out = nullptr;
        return Status::corruption();
    } else if (page_id.is_root()) {
        // The root is in memory for the duration of the transaction, and we don't bother with
        // its reference count.
        page_out = m_bufmgr.root();
        return Status::ok();
    } else if ((page_out = m_bufmgr.lookup(page_id))) {
        // Page is already in the cache. Do nothing.
    } else if ((s = ensure_available_buffer()).is_ok()) {
        // The page is not in the cache, and there is a buffer available to read it into.
        page_out = m_bufmgr.next_victim();
        page_out->page_id = page_id;
        m_bufmgr.register_page(*page_out);
        s = read_page(*page_out, nullptr);
    }
    if (s.is_ok()) {
        m_bufmgr.ref(*page_out);
    } else {
        page_out = nullptr;
    }
    return s;
}

auto Pager::get_unused_page(PageRef *&page_out) -> Status
{
    auto s = ensure_available_buffer();
    if (s.is_ok()) {
        // Increment the refcount, but don't register the page in the lookup table (we don't know its
        // page ID yet). That happens if/when the caller marks the page dirty before modifying it. At
        // that point, the page ID must be known.
        page_out = m_bufmgr.next_victim();
        m_bufmgr.ref(*page_out);
        CALICODB_EXPECT_EQ(page_out->flag, PageRef::kNormal);
        CALICODB_EXPECT_EQ(page_out->refs, 1);
    } else {
        page_out = nullptr;
    }
    return s;
}

auto Pager::get_root() -> PageRef &
{
    CALICODB_EXPECT_GE(m_mode, kRead);
    return *m_bufmgr.root();
}

auto Pager::mark_dirty(PageRef &page) -> void
{
    CALICODB_EXPECT_GE(m_mode, kWrite);
    if (page.get_flag(PageRef::kDirty)) {
        return;
    }
    m_dirtylist.add(page);
    if (m_mode == kWrite) {
        m_mode = kDirty;
    }
    if (!page.get_flag(PageRef::kCached)) {
        m_bufmgr.register_page(page);
    }
}

auto Pager::release(PageRef *&page, ReleaseAction action) -> void
{
    if (page) {
        CALICODB_EXPECT_GE(m_mode, kRead);
        if (!page->page_id.is_root()) {
            m_bufmgr.unref(*page);
            if (action < kKeep && page->refs == 0) {
                // kNoCache action is ignored if the page is dirty. It would just get written out
                // right now, but we shouldn't do anything that can fail in this routine.
                const auto is_dirty = page->get_flag(PageRef::kDirty);
                const auto should_discard = action == kDiscard || !is_dirty;
                if (should_discard) {
                    CALICODB_EXPECT_TRUE(page->get_flag(PageRef::kCached));
                    if (is_dirty) {
                        CALICODB_EXPECT_GE(m_mode, kDirty);
                        m_dirtylist.remove(*page);
                    }
                    m_bufmgr.erase(*page);
                }
            }
        }
        page = nullptr;
    }
}

auto Pager::initialize_root() -> void
{
    CALICODB_EXPECT_EQ(m_mode, kWrite);
    CALICODB_EXPECT_EQ(m_page_count, 0);
    m_page_count = 1;

    mark_dirty(get_root());
    FileHdr::make_supported_db(get_root().data, m_page_size);

    log(m_log, "initialized database root page");
}

auto Pager::refresh_state() -> Status
{
    // If this routine fails, the in-memory root page may be corrupted. Make sure that this routine is
    // called again to fix it.
    m_refresh = true;

    // Read the most-recent version of the database root page. This copy of the root may be located in
    // either the WAL, or the database file. If the database file is empty, and the WAL has never been
    // written, then a blank page is obtained here.
    size_t read_size;
    auto s = read_page(*m_bufmgr.root(), &read_size);
    if (!s.is_ok()) {
        return s;
    }
    auto *hdr = m_bufmgr.root()->data;
    if (read_size >= FileHdr::kSize) {
        // Make sure the file is a CalicoDB database, and that the database file format can be
        // understood by this version of the library.
        s = FileHdr::check_db_support(hdr);
        if (s.is_ok()) {
            m_page_count = FileHdr::get_page_count(hdr);
            // Set the database page size based on the value read from the file header.
            const auto new_page_size = FileHdr::get_page_size(hdr);
            if (m_page_size != new_page_size) {
                s = set_page_size(new_page_size);
                if (s.is_ok()) {
                    s = read_page(*m_bufmgr.root(), &read_size);
                }
            }
        }
    } else if (read_size > 0) {
        s = Status::corruption();
    }
    m_refresh = !s.is_ok();
    return s;
}

auto Pager::set_status(const Status &error) const -> void
{
    if (m_status->is_ok() && m_mode >= kWrite) {
        const auto is_fatal_error = error.is_io_error() ||
                                    error.is_corruption() ||
                                    error.is_aborted();
        if (is_fatal_error) {
            *m_status = error;
            m_mode = kError;

            log(m_log, "pager error: %s", error.message());
        }
    }
}

auto Pager::assert_state() const -> bool
{
    switch (m_mode) {
        case kOpen:
            CALICODB_EXPECT_EQ(m_bufmgr.refsum(), 0);
            CALICODB_EXPECT_TRUE(m_status->is_ok());
            CALICODB_EXPECT_TRUE(m_dirtylist.is_empty());
            break;
        case kRead:
            CALICODB_EXPECT_TRUE(m_status->is_ok());
            CALICODB_EXPECT_TRUE(m_dirtylist.is_empty());
            CALICODB_EXPECT_TRUE(m_bufmgr.assert_state());
            break;
        case kWrite:
            CALICODB_EXPECT_TRUE(m_status->is_ok());
            CALICODB_EXPECT_TRUE(m_dirtylist.is_empty());
            CALICODB_EXPECT_TRUE(m_bufmgr.assert_state());
            break;
        case kDirty:
            CALICODB_EXPECT_TRUE(m_status->is_ok());
            CALICODB_EXPECT_TRUE(m_bufmgr.assert_state());
            break;
        case kError:
            CALICODB_EXPECT_FALSE(m_status->is_ok());
            break;
        default:
            CALICODB_EXPECT_TRUE(false && "unrecognized Pager::Mode");
    }
    return true;
}

} // namespace calicodb
