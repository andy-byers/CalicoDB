// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "pager.h"
#include "calicodb/env.h"
#include "db_impl.h"
#include "encoding.h"
#include "freelist.h"
#include "header.h"
#include "logging.h"
#include "page.h"
#include "wal.h"

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
    Status s;

    // Try to read the page from the WAL.
    page = out.page;
    if ((s = m_wal->read(out.page_id, page)).is_ok()) {
        if (page == nullptr) {
            // No error, but the page could not be located in the WAL. Read the page
            // from the DB file instead.
            s = read_page_from_file(out, size_out);
        } else if (size_out) {
            *size_out = kPageSize;
        }
    }

    if (!s.is_ok()) {
        if (out.flag & PageRef::kDirty) {
            m_dirtylist.remove(out);
        }
        CALICODB_EXPECT_FALSE(dirtylist_contains(out));
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
        m_stats.stats[kStatRead] += slice.size();
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

    out = new Pager(param);
    return out->open_wal();
}

Pager::Pager(const Parameters &param)
    : m_status(param.status),
      m_db_name(param.db_name),
      m_wal_name(param.wal_name),
      m_sync_mode(param.sync_mode),
      m_lock_mode(param.lock_mode),
      m_bufmgr(param.frame_count),
      m_log(param.log),
      m_file(param.db_file),
      m_env(param.env),
      m_busy(param.busy)
{
    CALICODB_EXPECT_NE(m_env, nullptr);
    CALICODB_EXPECT_NE(m_file, nullptr);
    CALICODB_EXPECT_NE(m_status, nullptr);
}

Pager::~Pager()
{
    delete m_wal;
    delete m_file;
}

auto Pager::open_wal() -> Status
{
    const Wal::Parameters param = {
        m_wal_name,
        m_db_name,
        m_env,
        m_file,
        m_log,
        m_busy,
        m_sync_mode,
        m_lock_mode,
    };
    return Wal::open(param, m_wal);
}

auto Pager::close() -> Status
{
    finish();

    // This connection already has a shared lock on the DB file. Attempt to upgrade to an
    // exclusive lock, which, if successful would indicate that this is the only connection.
    // If this connection is using the Options::kLockExclusive lock mode, this call is a
    // NOOP, since the file is already locked in this mode.
    auto s = m_file->file_lock(kFileExclusive);
    if (s.is_ok()) {
        if (m_wal) {
            s = m_wal->close();
        }
        // Regardless of lock mode, this is where the database file lock is released. The
        // database file should not be accessed after this point.
        m_file->file_unlock();
    } else if (s.is_busy()) {
        s = Status::ok();
    }
    return s;
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
        if (changed || m_needs_refresh) {
            // Make sure there aren't any cached pages and that the root is up-to-date. Either
            // some other connection wrote to the database, or this connection rolled back the
            // last transaction (or both). Either way, we cannot trust anything in the cache.
            purge_pages(true);
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
    CALICODB_TRY(*m_status);

    Status s;
    if (m_mode == kDirty) {
        // Update the page count if necessary.
        auto root = acquire_root();
        if (FileHeader::get_page_count(root.constant_ptr()) != m_page_count) {
            mark_dirty(root);
            FileHeader::put_page_count(root.mutable_ptr(), m_page_count);
        }
        release(std::move(root));

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
    if (m_wal) {
        if (m_mode >= kDirty) {
            m_wal->rollback([this](auto id) {
                if (id.is_root()) {
                    m_needs_refresh = true;
                    return;
                }
                // Get rid of obsolete cached pages that aren't dirty anymore.
                if (auto *ref = m_bufmgr.get(id)) {
                    purge_page(*ref);
                }
            });
            m_wal->finish_writer();
            // Get rid of dirty pages.
            purge_pages(false);
        }
        m_wal->finish_reader();
    }
    *m_status = Status::ok();
    m_mode = kOpen;
}

auto Pager::purge_pages(bool purge_all) -> void
{
    for (auto *p = m_dirtylist.head; p;) {
        auto *save = p;
        p = p->next;
        m_dirtylist.remove(*save);
        if (save->page_id.is_root()) {
            // Indicate that the root needs to be reread.
            m_needs_refresh = true;
        } else {
            m_bufmgr.erase(save->page_id);
        }
    }
    CALICODB_EXPECT_EQ(m_dirtylist.head, nullptr);

    if (purge_all) {
        PageRef *victim;
        while ((victim = m_bufmgr.next_victim())) {
            CALICODB_EXPECT_NE(victim, nullptr);
            purge_page(*victim);
        }
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
            p = p->next;
        }
    }
    p = m_dirtylist.head;
    m_dirtylist.head = nullptr;
    CALICODB_EXPECT_NE(p, nullptr);

    // The DB page count is specified here. This indicates that the writes are part of
    // a commit, which is always the case if this method is called while the WAL is
    // enabled.
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
        // There are no available frames, so the cache must be full. "next_victim()" will not find
        // a page to evict if all pages are referenced, which should never happen.
        auto *victim = m_bufmgr.next_victim();
        CALICODB_EXPECT_NE(victim, nullptr);

        if (victim->flag & PageRef::kDirty) {
            CALICODB_EXPECT_EQ(m_mode, kDirty);
            m_dirtylist.remove(*victim);

            // Write just this page to the WAL. DB page count is 0 here because this write
            // is not part of a commit.
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

auto Pager::allocate(Page &page) -> Status
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
        const auto allocate_from_eof = [&page, this] {
            return acquire(Id::from_index(m_page_count), page);
        };
        s = allocate_from_eof();

        // Since this is a fresh page from the end of the file, it could be a pointer map page. If so,
        // it is already blank, so just skip it and allocate another. It'll get filled in as the pages
        // following it are used by the tree layer.
        if (s.is_ok() && PointerMap::lookup(page.id()) == page.id()) {
            release(std::move(page));
            s = allocate_from_eof();
        }
    } else if (s.is_ok()) {
        // id contains an unused page ID.
        s = acquire(id, page);
    }
    if (s.is_ok()) {
        // Callers of this routine will always modify `page`. Mark it dirty here for convenience.
        mark_dirty(page);
    }
    return s;
}

auto Pager::acquire(Id page_id, Page &page) -> Status
{
    CALICODB_EXPECT_GE(m_mode, kRead);
    if (page_id.is_null()) {
        return Status::corruption();
    }
    PageRef *ref;
    if (page_id.is_root()) {
        ref = m_bufmgr.root();
    } else if (!(ref = m_bufmgr.get(page_id))) {
        CALICODB_TRY(ensure_available_buffer());
        ref = m_bufmgr.alloc(page_id);
        if (page_id.as_index() < m_page_count) {
            CALICODB_TRY(read_page(*ref, nullptr));
        } else {
            std::memset(ref->page, 0, kPageSize);
            m_page_count = page_id.value;
        }
    }
    m_bufmgr.ref(*ref);
    page = Page(*this, *ref);
    return Status::ok();
}

auto Pager::destroy(Page page) -> Status
{
    CALICODB_EXPECT_GE(m_mode, kWrite);
    return Freelist::push(*this, std::move(page));
}

auto Pager::acquire_root() -> Page
{
    CALICODB_EXPECT_GE(m_mode, kRead);
    CALICODB_EXPECT_FALSE(m_needs_refresh);
    m_bufmgr.ref(*m_bufmgr.root());
    return Page(*this, *m_bufmgr.root());
}

auto Pager::mark_dirty(Page &page) -> void
{
    CALICODB_EXPECT_GE(m_mode, kWrite);
    if (!(page.m_ref->flag & PageRef::kDirty)) {
        m_dirtylist.add(*page.m_ref);
        if (m_mode == kWrite) {
            m_mode = kDirty;
        }
    }
    page.m_write = true;
}

auto Pager::release(Page page, ReleaseAction action) -> void
{
    CALICODB_EXPECT_GE(m_mode, kRead);
    if (page.m_pager) {
        page.m_pager = nullptr;
        auto *ref = page.m_ref;
        m_bufmgr.unref(*ref);
        if (action > kKeep && ref->refcount == 0) {
            // kNoCache action is ignored if the page is dirty. It would just get written out
            // right now, but we shouldn't do anything that can fail in this routine.
            const auto is_dirty = ref->flag & PageRef::kDirty;
            const auto is_discard = action == kDiscard || !is_dirty;
            if (is_discard) {
                if (is_dirty) {
                    m_dirtylist.remove(*ref);
                }
                m_bufmgr.erase(ref->page_id);
            }
        }
    }
}

auto Pager::initialize_root() -> void
{
    CALICODB_EXPECT_EQ(0, m_page_count);
    m_page_count = 1;

    // Initialize the file header.
    auto *root = m_bufmgr.root();
    FileHeader::make_supported_db(root->page);
}

auto Pager::refresh_state() -> Status
{
    Status s;
    // Read the most-recent version of the database root page. This copy of the root may be located in
    // either the WAL, or the database file. If the database file is empty, and the WAL has never been
    // written, then a blank page is obtained here.
    std::size_t read_size;
    s = read_page(*m_bufmgr.root(), &read_size);
    if (s.is_ok()) {
        m_needs_refresh = false;
        m_page_count = 0;
        m_save_count = 0;

        if (read_size == kPageSize) {
            // Make sure the file is a CalicoDB database, and that the database file format can be
            // understood by this version of the library.
            const auto *root = m_bufmgr.root()->page;
            s = FileHeader::check_db_support(root);
            if (s.is_ok()) {
                m_page_count = FileHeader::get_page_count(root);
                m_save_count = m_page_count;
            }
        } else if (read_size > 0) {
            s = Status::corruption();
        }
    }
    return s;
}

auto Pager::set_status(const Status &error) const -> Status
{
    if (m_status->is_ok() && (error.is_io_error() || error.is_corruption())) {
        *m_status = error;
        m_mode = kError;

        log(m_log, "pager error: %s", error.to_string().c_str());
    }
    return error;
}

auto Pager::assert_state() const -> bool
{
    switch (m_mode) {
        case kOpen:
            CALICODB_EXPECT_EQ(m_bufmgr.refsum(), 0);
            CALICODB_EXPECT_TRUE(m_status->is_ok());
            CALICODB_EXPECT_FALSE(m_dirtylist.head);
            break;
        case kRead:
            CALICODB_EXPECT_TRUE(m_status->is_ok());
            CALICODB_EXPECT_FALSE(m_dirtylist.head);
            break;
        case kWrite:
            CALICODB_EXPECT_TRUE(m_wal);
            break;
        case kDirty:
            CALICODB_EXPECT_TRUE(m_wal);
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
    for (auto *p = m_dirtylist.head; p; p = p->next) {
        CALICODB_EXPECT_TRUE(p->next == nullptr || p->next->prev == p);
        if (p->page_id == ref.page_id) {
            CALICODB_EXPECT_FALSE(found);
            found = true;
        }
    }
    return found;
}

static constexpr auto kEntrySize =
    sizeof(char) + // Type (1 B)
    Id::kSize;     // Back pointer (4 B)

static auto entry_offset(Id map_id, Id page_id) -> std::size_t
{
    CALICODB_EXPECT_LT(map_id, page_id);
    return (page_id.value - map_id.value - 1) * kEntrySize;
}

static auto decode_entry(const char *data) -> PointerMap::Entry
{
    PointerMap::Entry entry;
    entry.type = PointerMap::Type{*data++};
    entry.back_ptr.value = get_u32(data);
    return entry;
}

auto PointerMap::lookup(Id page_id) -> Id
{
    CALICODB_EXPECT_FALSE(page_id.is_null());

    // Root page (1) has no parents, and page 2 is the first pointer map page. If "page_id" is a pointer map
    // page, "page_id" will be returned.
    if (page_id.value < kFirstMapPage) {
        return Id::null();
    }
    const auto inc = kPageSize / kEntrySize + 1;
    const auto idx = (page_id.value - kFirstMapPage) / inc;
    return Id(idx * inc + kFirstMapPage);
}

auto PointerMap::is_map(Id page_id) -> bool
{
    return lookup(page_id) == page_id;
}

auto PointerMap::read_entry(Pager &pager, Id page_id, Entry &out) -> Status
{
    const auto mid = lookup(page_id);
    CALICODB_EXPECT_LE(kFirstMapPage, mid.value);
    CALICODB_EXPECT_NE(mid, page_id);

    const auto offset = entry_offset(mid, page_id);
    CALICODB_EXPECT_LE(offset + kEntrySize, kPageSize);

    Page map;
    CALICODB_TRY(pager.acquire(mid, map));
    out = decode_entry(map.constant_ptr() + offset);
    pager.release(std::move(map));
    return Status::ok();
}

auto PointerMap::write_entry(Pager &pager, Id page_id, Entry entry) -> Status
{
    const auto mid = lookup(page_id);
    CALICODB_EXPECT_LE(kFirstMapPage, mid.value);
    CALICODB_EXPECT_NE(mid, page_id);

    const auto offset = entry_offset(mid, page_id);
    CALICODB_EXPECT_LE(offset + kEntrySize, kPageSize);

    Page map;
    CALICODB_TRY(pager.acquire(mid, map));
    const auto [back_ptr, type] = decode_entry(map.constant_ptr() + offset);
    if (entry.back_ptr != back_ptr || entry.type != type) {
        pager.mark_dirty(map);
        auto *data = map.mutable_ptr() + offset;
        *data++ = entry.type;
        put_u32(data, entry.back_ptr.value);
    }
    pager.release(std::move(map));
    return Status::ok();
}

} // namespace calicodb
