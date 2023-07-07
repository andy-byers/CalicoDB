// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "pager.h"
#include "calicodb/env.h"
#include "freelist.h"
#include "header.h"
#include "logging.h"
#include "stat.h"
#include "temp.h"

namespace calicodb
{

auto Pager::purge_page(PageRef &victim) -> void
{
    if (victim.flag & PageRef::kDirty) {
        m_dirtylist.remove(victim);
    }
    CALICODB_EXPECT_FALSE(dirtylist_contains(victim));
    m_bufmgr.erase(victim.page_id);
}

auto Pager::read_page(PageRef &out, size_t *size_out) -> Status
{
    char *page = nullptr;

    // Try to read the page from the WAL.
    page = out.page;
    auto s = m_wal->read(out.page_id, page);
    if (s.is_ok()) {
        if (page == nullptr) {
            // No error, but the page could not be located in the WAL. Read the page
            // from the DB file instead.
            s = read_page_from_file(out, size_out);
        } else if (size_out) {
            *size_out = kPageSize;
        }
    }

    if (!s.is_ok()) {
        m_bufmgr.erase(out.page_id);
        if (m_mode > kRead) {
            set_status(s);
        }
    }
    return s;
}

auto Pager::read_page_from_file(PageRef &ref, std::size_t *size_out) const -> Status
{
    Slice slice;
    const auto offset = ref.page_id.as_index() * kPageSize;
    auto s = m_file->read(offset, kPageSize, ref.page, &slice);
    if (s.is_ok()) {
        m_stat->counters[Stat::kReadDB] += slice.size();
        std::memset(ref.page + slice.size(), 0, kPageSize - slice.size());
        if (size_out) {
            *size_out = slice.size();
        }
    }
    return s;
}

auto Pager::open(const Parameters &param, Pager *&out) -> Status
{
    CALICODB_EXPECT_GE(param.frame_count, kMinFrameCount);
    CALICODB_EXPECT_LE(param.frame_count * kPageSize, kMaxCacheSize);

    const Wal::Parameters wal_param = {
        param.wal_name,
        param.db_name,
        param.env,
        param.db_file,
        param.log,
        param.stat,
        param.busy,
        param.sync_mode,
        param.lock_mode,
    };
    Wal *wal;
    Status s;
    if (param.persistent) {
        s = Wal::open(wal_param, wal);
    } else {
        wal = new_temp_wal(wal_param);
    }
    if (s.is_ok()) {
        out = new Pager(*wal, param);
        if (0 == out->m_bufmgr.available()) {
            s = Status::invalid_argument("not enough memory for page cache");
            delete out;
        }
    }
    if (!s.is_ok()) {
        out = nullptr;
    }
    return s;
}

Pager::Pager(Wal &wal, const Parameters &param)
    : m_bufmgr(param.frame_count, *param.stat),
      m_status(param.status),
      m_log(param.log),
      m_file(param.db_file),
      m_wal(&wal),
      m_stat(param.stat),
      m_busy(param.busy)
{
    CALICODB_EXPECT_NE(m_file, nullptr);
    CALICODB_EXPECT_NE(m_status, nullptr);
    CALICODB_EXPECT_NE(m_stat, nullptr);
}

Pager::~Pager()
{
    finish();

    // This connection already has a shared lock on the DB file. Attempt to upgrade to an
    // exclusive lock, which, if successful would indicate that this is the only connection.
    // If this connection is using the Options::kLockExclusive lock mode, this call is a
    // NOOP, since the file is already locked in this mode.
    auto s = m_file->file_lock(kFileExclusive);
    if (s.is_ok() && m_wal) {
        s = m_wal->close();
    } else if (s.is_busy()) {
        s = Status::ok();
    }
    // Regardless of lock mode, this is where the database file lock is released. The
    // database file should not be accessed after this point.
    m_file->file_unlock();
    delete m_wal;

    if (!s.is_ok()) {
        log(m_log, "failed to close pager: %s", s.to_string().c_str());
    }
}

auto Pager::start_reader() -> Status
{
    CALICODB_EXPECT_NE(kError, m_mode);
    CALICODB_EXPECT_NE(nullptr, m_wal);
    CALICODB_EXPECT_TRUE(assert_state());

    if (m_mode != kOpen) {
        return Status::ok();
    }
    m_wal->finish_reader();

    bool changed;
    auto s = busy_wait(m_busy, [this, &changed] {
        return m_wal->start_reader(changed);
    });
    if (s.is_ok()) {
        if (changed) {
            // purge_pages(true) sets m_refresh unconditionally.
            purge_pages(true);
        }
        if (m_refresh) {
            s = refresh_state();
        }
        if (s.is_ok()) {
            m_page_count = FileHdr::get_page_count(
                m_bufmgr.root()->page);
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
        if (m_page_count != FileHdr::get_page_count(root.page)) {
            mark_dirty(root);
            FileHdr::put_page_count(root.page, m_page_count);
        }

        if (m_dirtylist.head == nullptr) {
            // Ensure that there is always a WAL frame to store the DB size.
            m_dirtylist.add(*m_bufmgr.root());
        }
        // Write all dirty pages to the WAL.
        s = flush_dirty_pages();
        if (s.is_ok()) {
            m_mode = kWrite;
        } else {
            set_status(s);
        }
    }
    return s;
}

auto Pager::finish() -> void
{
    CALICODB_EXPECT_TRUE(assert_state());

    if (m_mode >= kDirty) {
        m_wal->rollback([this](auto id) {
            if (id.is_root()) {
                m_refresh = true;
            } else if (auto *ref = m_bufmgr.get(id)) {
                // Get rid of obsolete cached pages that aren't dirty anymore.
                purge_page(*ref);
            }
        });
        m_wal->finish_writer();
        // Get rid of dirty pages, or all cached pages if there was a fault.
        purge_pages(m_mode == kError);
    }

    m_wal->finish_reader();
    *m_status = Status::ok();
    m_mode = kOpen;
}

auto Pager::purge_pages(bool purge_all) -> void
{
    for (auto *p = m_dirtylist.head; p;) {
        auto *save = p;
        p = p->next_dirty;
        m_dirtylist.remove(*save);
        if (save->page_id.is_root()) {
            m_refresh = true;
        } else {
            m_bufmgr.erase(save->page_id);
        }
    }
    CALICODB_EXPECT_EQ(m_dirtylist.head, nullptr);

    if (purge_all) {
        PageRef *victim;
        while ((victim = m_bufmgr.next_victim())) {
            CALICODB_EXPECT_NE(victim, nullptr);
            m_bufmgr.erase(victim->page_id);
        }
        // Indicate that the root page must be reread.
        m_refresh = true;
        CALICODB_EXPECT_EQ(m_bufmgr.occupied(), 0);
    }
}

auto Pager::checkpoint(bool reset) -> Status
{
    CALICODB_EXPECT_EQ(m_mode, kOpen);
    CALICODB_EXPECT_TRUE(assert_state());
    // Start and stop a read transaction to make sure the WAL index exists.
    auto s = start_reader();
    if (s.is_ok()) {
        finish();
    } else if (!s.is_busy()) {
        return s;
    }
    return m_wal->checkpoint(reset);
}

auto Pager::auto_checkpoint(std::size_t frame_limit) -> Status
{
    CALICODB_EXPECT_GT(frame_limit, 0);
    if (frame_limit < m_wal->last_frame_count()) {
        return checkpoint(false);
    }
    return Status::ok();
}

auto Pager::flush_dirty_pages() -> Status
{
    auto *p = m_dirtylist.head;
    while (p) {
        CALICODB_EXPECT_TRUE(p->flag & PageRef::kDirty);
        if (p->page_id.value > m_page_count) {
            // This page is past the current end of the file due to a vacuum operation
            // decreasing the page count. Just remove the page from the dirty list. It
            // wouldn't be transferred back to the DB on checkpoint anyway since it is
            // out of bounds.
            p = m_dirtylist.remove(*p);
        } else {
            p->flag = PageRef::kNormal;
            p = p->next_dirty;
        }
    }
    // These pages are no longer considered dirty. If the call to Wal::write() fails,
    // this connection must purge the whole cache.
    m_dirtylist.sort();
    p = m_dirtylist.head;
    m_dirtylist.head = nullptr;
    CALICODB_EXPECT_NE(p, nullptr);

    return m_wal->write(p, m_page_count);
}

auto Pager::set_page_count(U32 page_count) -> void
{
    for (auto i = page_count; i < m_page_count; ++i) {
        if (auto *out_of_range = m_bufmgr.query(Id::from_index(i))) {
            purge_page(*out_of_range);
        }
    }
    m_page_count = page_count;
}

auto Pager::ensure_available_buffer() -> Status
{
    Status s;
    if (!m_bufmgr.available()) {
        // There are no available frames, so the cache must be full. next_victim() will not find
        // a page to evict if all pages are referenced, which could happen if there are too many
        // cursors created on the same tree, each positioned on a different page.
        auto *victim = m_bufmgr.next_victim();
        if (victim == nullptr) {
            return Status::invalid_argument("out of page cache frames");
        }

        if (victim->flag & PageRef::kDirty) {
            CALICODB_EXPECT_EQ(m_mode, kDirty);
            m_dirtylist.remove(*victim);

            // Write just this page to the WAL. DB page count is 0 here because this write
            // is not part of a commit.
            victim->dirty = nullptr;
            s = m_wal->write(victim, 0);
            if (!s.is_ok()) {
                set_status(s);
            }
        }
        CALICODB_EXPECT_FALSE(dirtylist_contains(*victim));
        m_bufmgr.erase(victim->page_id);
    }
    return s;
}

auto Pager::allocate(PageRef *&page_out) -> Status
{
    CALICODB_EXPECT_GE(m_mode, kWrite);
    static constexpr std::size_t kMaxPageCount = 0xFF'FF'FF'FE;
    if (m_page_count == kMaxPageCount) {
        std::string message("reached the maximum allowed DB size (~");
        append_number(message, kMaxPageCount * kPageSize / 1'048'576);
        return Status::not_supported(message + " MB)");
    }

    // Try to get a page from the freelist first.
    Id id;
    auto s = Freelist::pop(*this, id);
    if (s.is_invalid_argument()) {
        // If the freelist was empty, get a page from the end of the file.
        const auto allocate_from_eof = [&page_out, this] {
            return acquire(Id::from_index(m_page_count), page_out);
        };
        s = allocate_from_eof();

        // Since this is a fresh page from the end of the file, it could be a pointer map page. If so,
        // it is already blank, so just skip it and allocate another. It'll get filled in as the pages
        // following it are used by the tree layer.
        if (s.is_ok() && PointerMap::is_map(page_out->page_id)) {
            release(page_out);
            s = allocate_from_eof();
        }
    } else if (s.is_ok()) {
        // id contains an unused page ID.
        s = acquire(id, page_out);
    }
    if (s.is_ok()) {
        // Callers of this routine will always modify `page`. Mark it dirty here for convenience.
        mark_dirty(*page_out);
    }
    return s;
}

auto Pager::acquire(Id page_id, PageRef *&page_out) -> Status
{
    CALICODB_EXPECT_GE(m_mode, kRead);
    page_out = nullptr;
    Status s;

    if (page_id.is_null() || m_page_count < page_id.as_index()) {
        // This allows `page_id` to be equal to the page count, which effectively adds a page
        // to the database. This is what Pager::allocate() does if the freelist is empty.
        return Status::corruption();
    } else if (page_id.is_root()) {
        // The root is in memory for the duration of the transaction, and we don't bother with
        // its reference count.
        page_out = m_bufmgr.root();
        return Status::ok();
    } else if (!(page_out = m_bufmgr.get(page_id)) &&
               (s = ensure_available_buffer()).is_ok()) {
        // The page is not in the cache, and there is a buffer available to read it into.
        page_out = m_bufmgr.alloc(page_id);
        if (page_id.as_index() < m_page_count) {
            s = read_page(*page_out, nullptr);
        } else {
            std::memset(page_out->page, 0, kPageSize);
            m_page_count = page_id.value;
        }
    }
    if (s.is_ok()) {
        m_bufmgr.ref(*page_out);
    }
    return s;
}

auto Pager::destroy(PageRef *&page) -> Status
{
    CALICODB_EXPECT_GE(m_mode, kWrite);
    return Freelist::push(*this, page);
}

auto Pager::get_root() -> PageRef &
{
    CALICODB_EXPECT_GE(m_mode, kRead);
    return *m_bufmgr.root();
}

auto Pager::mark_dirty(PageRef &page) -> void
{
    CALICODB_EXPECT_GE(m_mode, kWrite);
    if (!(page.flag & PageRef::kDirty)) {
        m_dirtylist.add(page);
        if (m_mode == kWrite) {
            m_mode = kDirty;
        }
    }
}

auto Pager::release(PageRef *&page, ReleaseAction action) -> void
{
    if (page) {
        CALICODB_EXPECT_GE(m_mode, kRead);
        if (!page->page_id.is_root()) {
            m_bufmgr.unref(*page);
            if (action < kKeep && page->refcount == 0) {
                // kNoCache action is ignored if the page is dirty. It would just get written out
                // right now, but we shouldn't do anything that can fail in this routine.
                const auto is_dirty = page->flag & PageRef::kDirty;
                const auto is_discard = action == kDiscard || !is_dirty;
                if (is_discard) {
                    if (is_dirty) {
                        m_dirtylist.remove(*page);
                    }
                    m_bufmgr.erase(page->page_id);
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
    m_mode = kDirty;

    // Initialize the file header.
    FileHdr::make_supported_db(get_root().page);
}

auto Pager::refresh_state() -> Status
{
    // If this routine fails, the in-memory root page may be corrupted. Make sure that this routine is
    // called again to fix it.
    m_refresh = true;

    Status s;
    // Read the most-recent version of the database root page. This copy of the root may be located in
    // either the WAL, or the database file. If the database file is empty, and the WAL has never been
    // written, then a blank page is obtained here.
    std::size_t read_size = 0;
    s = read_page(*m_bufmgr.root(), &read_size);
    if (s.is_ok()) {
        const auto *root = m_bufmgr.root()->page;
        if (read_size == kPageSize) {
            // Make sure the file is a CalicoDB database, and that the database file format can be
            // understood by this version of the library.
            s = FileHdr::check_db_support(root);
        } else if (read_size > 0) {
            s = Status::corruption();
        }
        if (s.is_ok()) {
            m_refresh = false;
        }
    }
    return s;
}

auto Pager::set_status(const Status &error) const -> void
{
    if (m_status->is_ok() && (error.is_io_error() || error.is_corruption())) {
        *m_status = error;
        m_mode = kError;

        log(m_log, "pager error: %s", error.to_string().c_str());
    }
}

auto Pager::assert_state() const -> bool
{
    switch (m_mode) {
        case kOpen:
            CALICODB_EXPECT_EQ(m_bufmgr.refsum(), 0);
            CALICODB_EXPECT_TRUE(m_status->is_ok());
            CALICODB_EXPECT_EQ(m_dirtylist.head, nullptr);
            break;
        case kRead:
            CALICODB_EXPECT_TRUE(m_status->is_ok());
            CALICODB_EXPECT_EQ(m_dirtylist.head, nullptr);
            break;
        case kWrite:
            CALICODB_EXPECT_TRUE(m_status->is_ok());
            CALICODB_EXPECT_EQ(m_dirtylist.head, nullptr);
            break;
        case kDirty:
            CALICODB_EXPECT_TRUE(m_status->is_ok());
            break;
        case kError:
            CALICODB_EXPECT_FALSE(m_status->is_ok());
            break;
        default:
            CALICODB_EXPECT_TRUE(false && "unrecognized Pager::Mode");
    }
    return true;
}

auto Pager::dirtylist_contains(const PageRef &ref) const -> bool
{
    auto found = false;
    for (auto *p = m_dirtylist.head; p; p = p->next_dirty) {
        CALICODB_EXPECT_TRUE(p->next_dirty == nullptr || p->next_dirty->prev_dirty == p);
        if (p->page_id == ref.page_id) {
            CALICODB_EXPECT_FALSE(found);
            found = true;
        }
    }
    return found;
}

static constexpr auto kEntrySize =
    sizeof(char) + // Type (1 B)
    sizeof(U32);   // Back pointer (4 B)

static auto entry_offset(Id map_id, Id page_id) -> std::size_t
{
    CALICODB_EXPECT_LT(map_id, page_id);
    return (page_id.value - map_id.value - 1) * kEntrySize;
}

static auto decode_entry(const char *data) -> PointerMap::Entry
{
    return {
        Id(get_u32(data + 1)),
        PointerMap::Type{*data},
    };
}

auto PointerMap::lookup(Id page_id) -> Id
{
    CALICODB_EXPECT_FALSE(page_id.is_null());

    // Root page (1) has no parents, and page 2 is the first pointer map page. If `page_id` is a pointer map
    // page, `page_id` will be returned.
    if (page_id.value < kFirstMapPage) {
        return Id::null();
    }
    static constexpr auto kMapSz = kPageSize / kEntrySize + 1;
    const auto idx = (page_id.value - kFirstMapPage) / kMapSz;
    return Id(idx * kMapSz + kFirstMapPage);
}

auto PointerMap::read_entry(Pager &pager, Id page_id, Entry &out) -> Status
{
    const auto mid = lookup(page_id);
    CALICODB_EXPECT_LE(kFirstMapPage, mid.value);
    CALICODB_EXPECT_NE(mid, page_id);

    const auto offset = entry_offset(mid, page_id);
    CALICODB_EXPECT_LE(offset + kEntrySize, kPageSize);

    PageRef *map;
    auto s = pager.acquire(mid, map);
    if (s.is_ok()) {
        out = decode_entry(map->page + offset);
        pager.release(map);
    }
    return s;
}

auto PointerMap::write_entry(Pager &pager, Id page_id, Entry entry) -> Status
{
    const auto mid = lookup(page_id);
    CALICODB_EXPECT_LE(kFirstMapPage, mid.value);
    CALICODB_EXPECT_NE(mid, page_id);

    const auto offset = entry_offset(mid, page_id);
    CALICODB_EXPECT_LE(offset + kEntrySize, kPageSize);

    PageRef *map;
    auto s = pager.acquire(mid, map);
    if (s.is_ok()) {
        const auto [back_ptr, type] = decode_entry(
            map->page + offset);
        if (entry.back_ptr != back_ptr || entry.type != type) {
            pager.mark_dirty(*map);
            auto *data = map->page + offset;
            *data++ = entry.type;
            put_u32(data, entry.back_ptr.value);
        }
        pager.release(map);
    }
    return s;
}

} // namespace calicodb
