// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "pager.h"
#include "calicodb/env.h"
#include "calicodb/wal.h"
#include "header.h"
#include "logging.h"
#include "mem.h"
#include "node.h"
#include "status_internal.h"
#include "temp.h"
#include "wal_internal.h"

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
    const auto scratch_size = value * kScratchBufferPages;
    if (m_scratch.realloc(scratch_size)) {
        return Status::no_memory();
    }
    if (m_bufmgr.reallocate(value)) {
        return Status::no_memory();
    }
    m_page_size = value;
    std::memset(m_scratch.data(), 0, scratch_size);
    log(m_log, "database page size is set to %u", value);
    return Status::ok();
}

void Pager::purge_page(PageRef &victim)
{
    if (victim.get_flag(PageRef::kDirty)) {
        m_dirtylist.remove(victim);
    }
    m_bufmgr.erase(victim);
}

auto Pager::read_page(PageRef &page_out, size_t *size_out) -> Status
{
    Status s;
    // Try to read the page from the WAL.
    auto *page = page_out.data;
    if (m_wal) {
        s = m_wal->read(page_out.page_id.value, m_page_size, page);
    } else {
        // Indicate that this page must be read from the database file. If the WAL is
        // not open at this point, then it must not have existed at the time this
        // transaction was started.
        page = nullptr;
    }
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
    const auto offset = ref.page_id.as_index() * static_cast<uint64_t>(m_page_size);
    auto s = m_file->read(offset, m_page_size, ref.data, &slice);
    if (s.is_ok()) {
        m_stats->read_db += slice.size();
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
    const WalOptions options = {
        m_env,
        m_file,
        m_stats,
    };
    // Extra parameters for WAL class constructors. Members are const in WalImpl and
    // TempWal (the default and in-memory WAL implementations, respectively).
    const WalOptionsExtra extra = {
        options,
        m_log,
        m_sync_mode,
        m_lock_mode,
    };
    if (m_user_wal) {
        m_wal = m_user_wal;
    } else {
        m_wal = new_default_wal(extra, m_wal_name);
        if (m_wal == nullptr) {
            return Status::no_memory();
        }
    }
    CALICODB_EXPECT_NE(m_wal, nullptr);
    auto s = m_wal->open(options, m_wal_name);
    if (!s.is_ok()) {
        // The WAL must never be left in "Closed" mode. If the WAL is not "Open", m_wal
        // must be equal to nullptr.
        if (!m_user_wal) {
            Mem::delete_object(m_wal);
        }
        m_wal = nullptr;
    }
    return s;
}

auto Pager::open_wal_if_present() -> Status
{
    Status s;
    if (m_persistent && m_env->file_exists(m_wal_name)) {
        s = open_wal();
    }
    return s;
}

auto Pager::close_wal() -> Status
{
    CALICODB_EXPECT_EQ(m_mode, kOpen);
    Status s;
    if (!m_wal && m_env->file_exists(m_wal_name)) {
        CALICODB_EXPECT_TRUE(m_persistent);
        s = open_wal();
    }
    if (s.is_ok() && m_wal) {
        // This connection already has a shared lock on the DB file. Attempt to upgrade to an
        // exclusive lock, which, if successful would indicate that this is the only connection.
        // If this connection is using the Options::kLockExclusive lock mode, this call is a
        // NOOP, since the file is already locked in this mode. Released in Pager::close().
        s = m_file->file_lock(kFileExclusive);
        if (s.is_ok()) {
            s = m_wal->close(m_scratch.data(), m_page_size); // TODO: Page size may not be correct if a transaction was never started.
        } else if (s.is_busy()) {
            s = Status::ok();
        }
    }
    if (m_wal != m_user_wal) {
        Mem::delete_object(m_wal);
    }
    m_wal = nullptr;
    return s;
}

auto Pager::open(const Parameters &param, Pager *&pager_out) -> Status
{
    Status s;
    pager_out = Mem::new_object<Pager>(param);
    if (pager_out) {
        s = pager_out->set_page_size(param.page_size);
    } else {
        s = Status::no_memory();
    }
    if (!s.is_ok()) {
        Mem::delete_object(pager_out);
        pager_out = nullptr;
    }
    return s;
}

Pager::Pager(const Parameters &param)
    : m_bufmgr((param.cache_size + param.page_size - 1) / param.page_size, *param.stat),
      m_status(param.status),
      m_log(param.log),
      m_env(param.env),
      m_user_wal(param.wal),
      m_file(param.db_file),
      m_stats(param.stat),
      m_busy(param.busy),
      m_lock_mode(param.lock_mode),
      m_sync_mode(param.sync_mode),
      m_persistent(param.persistent),
      m_db_name(param.db_name),
      m_wal_name(param.wal_name),
      m_wal(param.wal)
{
    CALICODB_EXPECT_NE(m_file, nullptr);
    CALICODB_EXPECT_NE(m_status, nullptr);
    CALICODB_EXPECT_NE(m_stats, nullptr);
    CALICODB_EXPECT_NE(m_db_name, nullptr);
    CALICODB_EXPECT_NE(m_wal_name, nullptr);
}

Pager::~Pager()
{
    CALICODB_EXPECT_EQ(m_mode, kOpen);
    CALICODB_EXPECT_TRUE(m_status->is_ok());
    if (m_wal != m_user_wal) {
        Mem::delete_object(m_wal);
    }
}

void Pager::close()
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

auto Pager::lock_reader_impl(bool refresh) -> Status
{
    Status s;
    if (refresh) {
        purge_pages(true);
    }
    if (m_refresh) {
        s = refresh_state();
    }
    if (s.is_ok()) {
        m_mode = kRead;
    } else {
        finish();
    }
    return s;
}

auto Pager::lock_reader(bool *changed_out) -> Status
{
    CALICODB_EXPECT_LE(m_mode, kRead);
    CALICODB_EXPECT_TRUE(assert_state());

    Status s;
    if (m_wal) {
        m_wal->finish_read();
    } else {
        s = open_wal_if_present();
    }
    bool changed = false;
    if (m_wal && s.is_ok()) {
        // Start a read transaction on the WAL. If Wal::start_read() indicates that the WAL
        // is busy, use m_busy to wait and try again.
        s = busy_wait(m_busy, [this, &changed] {
            return m_wal->start_read(changed);
        });
    }
    if (s.is_ok()) {
        s = lock_reader_impl(changed);
    }
    if (changed_out) {
        *changed_out = changed;
    }
    return s;
}

auto Pager::begin_writer() -> Status
{
    CALICODB_EXPECT_NE(m_mode, kOpen);
    CALICODB_EXPECT_NE(m_mode, kError);
    CALICODB_EXPECT_TRUE(assert_state());

    Status s;
    if (m_mode == kRead) {
        if (m_wal == nullptr) {
            s = open_wal();
            if (s.is_ok()) {
                // lock_reader() must have been called at some point to get the pager
                // into kRead mode. The WAL must not have existed at that point. It does
                // now, so call lock_reader() again to start a read transaction on it.
                s = lock_reader(nullptr);
            }
        }
        if (s.is_ok()) {
            s = m_wal->start_write();
        }
        if (!s.is_ok()) {
            return s;
        }
        uint32_t page_count;
        s = get_page_count(page_count);
        if (s.is_ok()) {
            m_mode = kWrite;
            m_page_count = page_count;
            m_saved_page_count = page_count;
            if (page_count == 0) {
                initialize_root();
            }
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

void Pager::move_page(PageRef &page, Id destination)
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

void Pager::undo_callback(void *arg, uint32_t key)
{
    const Id id(key);
    if (id.is_root()) {
        return;
    }
    auto *pager = static_cast<Pager *>(arg);
    if (auto *ref = pager->m_bufmgr.query(id)) {
        pager->purge_page(*ref);
    }
}

void Pager::finish()
{
    CALICODB_EXPECT_TRUE(assert_state());

    if (m_mode >= kWrite) {
        if (m_mode == kDirty) {
            // Get rid of obsolete cached pages that aren't dirty anymore.
            m_wal->rollback(undo_callback, this);
        }
        m_wal->finish_write();
        // Get rid of dirty pages, or all cached pages if there was a fault.
        purge_pages(m_mode == kError);
        m_page_count = m_saved_page_count;
    }
    if (m_mode >= kRead) {
        m_bufmgr.shrink_to_fit();
    }
    if (m_wal) {
        m_wal->finish_read();
    }
    *m_status = Status::ok();
    m_mode = kOpen;
}

void Pager::purge_pages(bool purge_all)
{
    for (auto *dirty = m_dirtylist.begin(); dirty != m_dirtylist.end();) {
        auto *save = dirty->get_page_ref();
        if (save->page_id.is_root()) {
            m_refresh = true;
        }
        dirty = dirty->next_entry;
        purge_page(*save);
    }
    CALICODB_EXPECT_TRUE(m_dirtylist.is_empty());

    if (purge_all) {
        m_bufmgr.purge();
        m_refresh = true;
    }
}

auto Pager::checkpoint(CheckpointMode mode, CheckpointInfo *info_out) -> Status
{
    CALICODB_EXPECT_EQ(m_mode, kOpen);
    CALICODB_EXPECT_TRUE(assert_state());
    if (m_wal == nullptr) {
        // Ensure that the WAL and WAL index have been created.
        auto s = lock_reader(nullptr);
        if (!s.is_ok()) {
            return s;
        }
        finish();
    }
    if (m_wal) {
        return m_wal->checkpoint(mode, m_scratch.data(), m_page_size,
                                 mode == kCheckpointPassive ? nullptr : m_busy,
                                 info_out);
    }
    return Status::ok();
}

auto Pager::auto_checkpoint(size_t frame_limit) -> Status
{
    CALICODB_EXPECT_GT(frame_limit, 0);
    if (m_wal && frame_limit < m_wal->callback()) {
        return checkpoint(kCheckpointFull, nullptr);
    }
    return Status::ok();
}

auto Pager::flush_dirty_pages() -> Status
{
    auto *dirty = m_dirtylist.begin();
    while (dirty != m_dirtylist.end()) {
        auto *page = dirty->get_page_ref();
        CALICODB_EXPECT_TRUE(page->get_flag(PageRef::kDirty));
        if (page->page_id.value <= m_page_count) {
            page->clear_flag(PageRef::kDirty);
            dirty = dirty->next_entry;
        } else {
            // This page is past the current end of the file due the page count having
            // been decreased. Just remove the page from the dirty list. It wouldn't be
            // transferred back to the DB on checkpoint anyway since it is out of bounds.
            dirty = m_dirtylist.remove(*page);
        }
    }
    // These pages are no longer considered dirty. If the call to Wal::write() fails,
    // this connection must purge the whole cache.
    dirty = m_dirtylist.sort();
    CALICODB_EXPECT_NE(dirty, nullptr);

    WalPagesImpl pages(*dirty->get_page_ref());
    return m_wal->write(pages, m_page_size, m_page_count);
}

void Pager::set_page_count(uint32_t page_count)
{
    CALICODB_EXPECT_GT(page_count, 0);
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

        WalPagesImpl pages(*victim);
        // DB page count is 0 here because this write is not part of a commit.
        s = m_wal->write(pages, m_page_size, 0);
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
        CALICODB_EXPECT_FALSE(page_out->page_id.is_root());
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
        return StatusBuilder::corruption("page %u is out of bounds (page count is %u)",
                                         page_id.value, m_page_count);
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
        if (page_out->page_id.is_null()) {
            // If this PageRef has never held a valid database page, then its contents will be
            // uninitialized. Make sure not to let uninitialized data into the file: it makes
            // it impossible to reproduce results.
            std::memset(page_out->data, 0, m_page_size);
        }
    } else {
        page_out = nullptr;
    }
    return s;
}

auto Pager::get_root() -> PageRef &
{
    CALICODB_EXPECT_GE(m_mode, kRead);
    CALICODB_EXPECT_GT(m_page_count, 0);
    return *m_bufmgr.root();
}

void Pager::mark_dirty(PageRef &page)
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

void Pager::release(PageRef *&page, ReleaseAction action)
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

void Pager::initialize_root()
{
    CALICODB_EXPECT_EQ(m_mode, kWrite);
    CALICODB_EXPECT_EQ(m_page_count, 0);

    m_page_count = 1;
    auto &root = get_root();

    // Initialize the root page of the database in the special buffer allocated by the
    // buffer manager. Page count is written in commit().
    mark_dirty(root);
    FileHdr::make_supported_db(root.data, m_page_size);

    log(m_log, "initialized database");
}

auto Pager::get_page_count(uint32_t &value_out) -> Status
{
    // Try to determine the page count from the WAL. Value is known if the WAL is open
    // and has at least 1 transaction committed.
    value_out = m_wal ? m_wal->db_size() : 0;
    if (value_out == 0) {
        uint64_t file_size;
        // Use the actual file size, rounded up to the nearest page.
        auto s = m_file->get_size(file_size);
        if (s.is_ok()) {
            value_out = static_cast<uint32_t>(
                (file_size + m_page_size - 1) / m_page_size);
        }
        return s;
    }
    return Status::ok();
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
            s = get_page_count(m_page_count);
        }
        if (s.is_ok()) {
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
        s = StatusBuilder::corruption("incomplete root page (read %u bytes)",
                                      read_size);
    }
    m_refresh = !s.is_ok();
    return s;
}

void Pager::set_status(const Status &error) const
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
#ifndef NDEBUG
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
#endif // NDEBUG
    return true;
}

} // namespace calicodb
