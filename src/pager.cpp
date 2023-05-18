// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "pager.h"
#include "db_impl.h"
#include "encoding.h"
#include "header.h"
#include "logging.h"
#include "page.h"
#include "wal.h"
#include <thread>

namespace calicodb
{

auto Pager::hits() const -> U64
{
    return m_bufmgr.hits();
}

auto Pager::misses() const -> U64
{
    return m_bufmgr.misses();
}

auto Pager::mode() const -> Mode
{
    return m_mode;
}

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
        m_statistics.bytes_read += slice.size();
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
    : m_state(param.state),
      m_db_name(param.db_name),
      m_wal_name(param.wal_name),
      m_sync(param.sync),
      m_freelist(*this, Id::null()),
      m_bufmgr(param.frame_count),
      m_log(param.log),
      m_file(param.db_file),
      m_env(param.env),
      m_busy(param.busy)
{
    CALICODB_EXPECT_NE(m_env, nullptr);
    CALICODB_EXPECT_NE(m_file, nullptr);
    CALICODB_EXPECT_NE(m_state, nullptr);
}

Pager::~Pager()
{
    delete m_wal;
    delete m_file;
}

auto Pager::statistics() const -> const Statistics &
{
    return m_statistics;
}

auto Pager::wal_statistics() const -> WalStatistics
{
    return m_wal ? m_wal->statistics() : WalStatistics{};
}

auto Pager::page_count() const -> std::size_t
{
    return m_page_count;
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
        m_sync,
    };
    return Wal::open(param, m_wal);
}

auto Pager::close() -> Status
{
    finish();
    if (m_mode == kError) {
        // Skip the checkpoint if this connection encountered a fatal error.
        return m_state->status;
    }

    // This connection already has a shared lock on the DB file. Attempt to upgrade to an
    // exclusive lock, which, if successful would indicate that this is the only connection.
    auto s = m_file->file_lock(kLockExclusive);
    if (s.is_ok()) {
        if (m_wal) {
            // Ignore the page count output by Wal::close(). The DB is being closed, so
            // it isn't necessary to set.
            std::size_t unused;
            s = m_wal->close(unused);
        }
        m_file->file_unlock();
    }
    return s;
}

auto Pager::start_reader() -> Status
{
    CALICODB_EXPECT_NE(kError, m_mode);
    CALICODB_EXPECT_NE(nullptr, m_wal);
    CALICODB_EXPECT_TRUE(assert_state());

    Status s;
    if (m_mode != kOpen) {
        return s;
    }
    if (s.is_ok()) {
        m_wal->finish_reader();

        bool changed;
        s = m_wal->start_reader(changed);
        if (s.is_ok()) {
            if (changed) {
                purge_cached_pages();
            }
            m_mode = kRead;
            m_refresh_root = true;
            s = refresh_state();
            if (s.is_ok()) {
                m_save.mode = kRead;
                m_save.page_count = m_page_count;
                m_save.freelist_head = m_freelist.m_head;
            }
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
            m_save.mode = kWrite;
        }
    }
    return s;
}

auto Pager::commit() -> Status
{
    CALICODB_EXPECT_NE(m_mode, kOpen);
    CALICODB_EXPECT_TRUE(assert_state());
    set_status(refresh_state());

    // Report prior errors again.
    CALICODB_TRY(m_state->status);

    Status s;
    if (m_mode == kDirty) {
        // Write the file header to the root page if anything has changed.
        auto root = acquire_root();
        const auto needs_new_header =
            m_page_count != get_u32(root.data() + FileHeader::kPageCountOffset) ||
            m_freelist.m_head.value != get_u32(root.data() + FileHeader::kFreelistHeadOffset);
        if (needs_new_header) {
            mark_dirty(root);
            put_u32(root.data() + FileHeader::kPageCountOffset, static_cast<U32>(m_page_count));
            put_u32(root.data() + FileHeader::kFreelistHeadOffset, m_freelist.m_head.value);
        }
        release(std::move(root));

        if (m_dirtylist.head == nullptr) {
            // Ensure that there is always a WAL frame to store the DB size.
            m_dirtylist.add(*m_bufmgr.root());
        }

        s = flush_all_pages();
        if (s.is_ok()) {
            CALICODB_EXPECT_FALSE(m_dirtylist.head);
            m_save.freelist_head = m_freelist.m_head;
            m_save.page_count = m_page_count;
        }
        set_status(s);
    }
    if (s.is_ok()) {
        m_mode = m_save.mode;
    }
    return s;
}

auto Pager::rollback() -> void
{
    CALICODB_EXPECT_NE(m_mode, kOpen);
    CALICODB_EXPECT_TRUE(assert_state());

    if (m_mode >= kDirty) {
        m_wal->rollback();
        m_state->status = Status::ok();
        m_freelist.m_head = m_save.freelist_head;
        m_page_count = m_save.page_count;
        m_mode = m_save.mode;
        purge_cached_pages();
    }
    m_refresh_root = true;

    // State variables values should match what they were at the start of the
    // transaction, or after the last commit. If this is a read-only transaction,
    // none of these values are allowed to change.
    CALICODB_EXPECT_EQ(m_freelist.m_head, m_save.freelist_head);
    CALICODB_EXPECT_EQ(m_page_count, m_save.page_count);
    CALICODB_EXPECT_EQ(m_mode, m_save.mode);
}

auto Pager::finish() -> void
{
    if (m_wal) {
        if (m_mode >= kDirty) {
            rollback();
        }
        m_wal->finish_reader();
    }
    m_mode = kOpen;
    m_save = {};
}

auto Pager::purge_cached_pages() -> void
{
    PageRef *victim;
    while ((victim = m_bufmgr.next_victim())) {
        CALICODB_EXPECT_NE(victim, nullptr);
        purge_page(*victim);
    }
    CALICODB_EXPECT_EQ(m_bufmgr.size(), 0);
    if (m_dirtylist.head) {
        CALICODB_EXPECT_EQ(m_dirtylist.head, m_bufmgr.root());
        CALICODB_EXPECT_FALSE(m_dirtylist.head->prev);
        CALICODB_EXPECT_FALSE(m_dirtylist.head->next);
        m_dirtylist.head->flag = PageRef::kNormal;
        m_dirtylist.head = nullptr;
    }
}

auto Pager::checkpoint(bool reset) -> Status
{
    CALICODB_EXPECT_EQ(m_mode, kOpen);
    CALICODB_EXPECT_TRUE(assert_state());
    bool ignore;
    auto s = m_wal->start_reader(ignore);
    if (s.is_ok()) {
        m_wal->finish_reader();
    } else if (!s.is_busy()) {
        return s;
    }
    return m_wal->checkpoint(reset);
}

auto Pager::flush_all_pages() -> Status
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

auto Pager::set_status(const Status &error) const -> Status
{
    if (m_state->status.is_ok() && (error.is_io_error() || error.is_corruption())) {
        m_state->status = error;
        m_mode = kError;
    }
    return error;
}

auto Pager::set_page_count(std::size_t page_count) -> void
{
    CALICODB_EXPECT_GT(page_count, 0);
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
            if (!(s = m_wal->write(victim, 0)).is_ok()) {
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
    if (!m_freelist.is_empty()) {
        return m_freelist.pop(page);
    }

    const auto allocate_upgraded = [&page, this] {
        auto s = acquire(Id::from_index(m_page_count), page);
        if (s.is_ok()) {
            mark_dirty(page);
        }
        return s;
    };
    CALICODB_TRY(allocate_upgraded());

    // Since this is a fresh page from the end of the file, it could be a pointer map page. If so,
    // it is already blank, so just skip it and allocate another. It'll get filled in as the pages
    // following it are used by the tree layer.
    if (PointerMap::lookup(page.id()) == page.id()) {
        release(std::move(page));
        return allocate_upgraded();
    }
    return Status::ok();
}

auto Pager::acquire(Id page_id, Page &page) -> Status
{
    CALICODB_EXPECT_GE(m_mode, kRead);
    CALICODB_EXPECT_FALSE(page_id.is_null());
    CALICODB_TRY(refresh_state());

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
    return m_freelist.push(std::move(page));
}

auto Pager::acquire_root() -> Page
{
    CALICODB_EXPECT_GE(m_mode, kRead);
    CALICODB_EXPECT_FALSE(m_refresh_root);
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
    CALICODB_EXPECT_FALSE(page.m_write);
    page.m_write = true;
}

auto Pager::release(Page page) -> void
{
    CALICODB_EXPECT_GE(m_mode, kRead);
    CALICODB_EXPECT_GT(page.m_ref->refcount, 0);
    m_bufmgr.unref(*page.m_ref);
    page.m_pager = nullptr;
}

auto Pager::initialize_root() -> void
{
    CALICODB_EXPECT_FALSE(m_refresh_root);
    CALICODB_EXPECT_EQ(0, m_page_count);
    m_page_count = 1;

    // Initialize the file header.
    auto *root = m_bufmgr.root();
    FileHeader::make_supported_db(root->page);
}

auto Pager::refresh_state() -> Status
{
    Status s;
    if (m_refresh_root) {
        // Read the most-recent version of the database root page. This copy of the root may be located in
        // either the WAL, or the database file. If the database file is empty, and the WAL has never been
        // written, then a blank page is obtained here.
        std::size_t read_size;
        if ((s = read_page(*m_bufmgr.root(), &read_size)).is_ok()) {
            m_refresh_root = false;

            if (read_size) {
                // Make sure the file is a CalicoDB database, and that the database file format can be
                // understood by this version of the library.
                const auto *root = m_bufmgr.root()->page;
                if ((s = FileHeader::check_db_support(root)).is_ok()) {
                    // Decode the file header fields necessary for execution.
                    m_page_count = get_u32(root + FileHeader::kPageCountOffset);
                    m_freelist.m_head.value = get_u32(root + FileHeader::kFreelistHeadOffset);
                }
            }
            m_save.freelist_head = m_freelist.m_head;
            m_save.page_count = m_page_count;
        }
    }
    return s;
}

auto Pager::assert_state() const -> bool
{
    switch (m_mode) {
        case kOpen:
            CALICODB_EXPECT_EQ(m_bufmgr.refsum(), 0);
            CALICODB_EXPECT_TRUE(m_state->status.is_ok());
            CALICODB_EXPECT_FALSE(m_dirtylist.head);
            break;
        case kRead:
            CALICODB_EXPECT_TRUE(m_state->status.is_ok());
            CALICODB_EXPECT_FALSE(m_dirtylist.head);
            break;
        case kWrite:
            CALICODB_EXPECT_TRUE(m_wal);
            break;
        case kDirty:
            CALICODB_EXPECT_TRUE(m_wal);
            break;
        case kError:
            CALICODB_EXPECT_FALSE(m_state->status.is_ok());
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

// The first pointer map page is always on page 2, right after the root page.
static constexpr std::size_t kFirstMapPage = 2;

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
    out = decode_entry(map.data() + offset);
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
    const auto [back_ptr, type] = decode_entry(map.data() + offset);
    if (entry.back_ptr != back_ptr || entry.type != type) {
        if (!map.is_writable()) {
            pager.mark_dirty(map);
        }
        auto data = map.data() + offset;
        *data++ = entry.type;
        put_u32(data, entry.back_ptr.value);
    }
    pager.release(std::move(map));
    return Status::ok();
}

Freelist::Freelist(Pager &pager, Id head)
    : m_pager(&pager),
      m_head(head)
{
}

[[nodiscard]] auto Freelist::is_empty() const -> bool
{
    return m_head.is_null();
}

auto Freelist::pop(Page &page) -> Status
{
    CALICODB_EXPECT_FALSE(m_head.is_null());
    CALICODB_TRY(m_pager->acquire(m_head, page));
    m_pager->mark_dirty(page);
    m_head = read_next_id(page);

    if (!m_head.is_null()) {
        // Only clear the back pointer for the new freelist head. Callers must make sure to update the returned
        // node's back pointer at some point.
        const PointerMap::Entry entry = {Id::null(), PointerMap::kFreelistLink};
        CALICODB_TRY(PointerMap::write_entry(*m_pager, m_head, entry));
    }
    return Status::ok();
}

auto Freelist::push(Page page) -> Status
{
    CALICODB_EXPECT_FALSE(page.id().is_root());
    write_next_id(page, m_head);

    // Write the parent of the old head, if it exists.
    PointerMap::Entry entry = {page.id(), PointerMap::kFreelistLink};
    if (!m_head.is_null()) {
        CALICODB_TRY(PointerMap::write_entry(*m_pager, m_head, entry));
    }
    // Clear the parent of the new head.
    entry.back_ptr = Id::null();
    CALICODB_TRY(PointerMap::write_entry(*m_pager, page.id(), entry));

    m_head = page.id();
    m_pager->release(std::move(page));
    return Status::ok();
}

} // namespace calicodb
