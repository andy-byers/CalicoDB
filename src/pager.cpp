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

auto Pager::mode() const -> Mode
{
    return m_mode;
}

auto Pager::purge_entry(CacheEntry &victim) -> void
{
    if (victim.is_dirty) {
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
            set_status(s);
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
    Slice slice;
    const auto offset = entry.page_id.as_index() * m_frames.page_size();
    auto s = m_file->read(offset, m_frames.page_size(), entry.page, &slice);
    if (m_mode > kOpen) { // TODO: Should be kRead, once that mode exists.
        set_status(s);
    }
    m_bytes_read += slice.size();
    std::memset(entry.page + slice.size(), 0, m_frames.page_size() - slice.size());
    return s;
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

    // The rest of this method serves to allocate/acquire the root page (the
    // first page in the file) via the usual routines, then move its cache
    // entry to a dedicated member variable. The root page stays in memory
    // as long as the pager is alive (it is refreshed after a rollback).
    Page p;
    Status s;
    if (exists) {
        s = out->acquire(Id::root(), p);
    } else {
        s = out->allocate(p);
    }
    if (!s.is_ok()) {
        delete out;
        out = nullptr;
    }
    // Copy metadata and frame pointer.
    out->m_root = *p.entry();
    out->release(std::move(p));
    out->m_cache.erase(Id::root());
    if (out->m_dirty) {
        // If this is a new file, the root page is dirty since it was just
        // allocated. Fix the invalid dirty list reference.
        CALICODB_EXPECT_FALSE(exists);
        out->m_dirty = &out->m_root;
        out->m_mode = kDirty;
        CALICODB_EXPECT_FALSE(out->m_dirty->prev);
        CALICODB_EXPECT_FALSE(out->m_dirty->next);
    }
    return s;
}

Pager::Pager(const Parameters &param, File &file, AlignedBuffer buffer)
    : m_state(param.state),
      m_filename(param.filename),
      m_frames(std::move(buffer), param.page_size, param.frame_count),
      m_freelist(*this, m_state->freelist_head),
      m_log(param.log),
      m_file(&file),
      m_env(param.env),
      m_wal(param.wal)
{
    CALICODB_EXPECT_NE(m_state, nullptr);
    m_root.page_id = Id::root();
    m_frames.pin(m_root);
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
        CALICODB_EXPECT_FALSE(m_dirty->prev);
        m_dirty->prev = &entry;
    }
    entry.is_dirty = true;
    entry.prev = nullptr;
    entry.next = m_dirty;
    m_dirty = &entry;
}

auto Pager::clean_page(CacheEntry &entry) -> CacheEntry *
{
    CALICODB_EXPECT_TRUE(m_dirty);
    CALICODB_EXPECT_FALSE(m_dirty->prev);
    CALICODB_EXPECT_TRUE(entry.is_dirty);
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
    CALICODB_EXPECT_NE(m_mode, kOpen);
    if (m_mode <= kWrite) {
        // No work done in this transaction, or not even in a transaction. Nothing should be
        // modified.
        CALICODB_EXPECT_EQ(m_dirty, nullptr);
        m_mode = kOpen;
        return Status::ok();
    }
    // Report prior errors again.
    CALICODB_TRY(m_state->status);

    if (m_mode == kDirty) {
        // Write the file header to the root page if anything has changed.
        FileHeader header;
        auto root = acquire_root();
        header.read(root.data());
        const auto needs_new_header =
            header.page_count != m_page_count ||
            header.freelist_head != m_state->freelist_head.value;
        if (needs_new_header) {
            upgrade(root);
            header.page_count = static_cast<U32>(m_page_count);
            header.freelist_head = m_state->freelist_head.value;
            header.write(root.data());
        }
        release(std::move(root));

        if (m_dirty == nullptr) {
            // Ensure that there is always a WAL frame to store the DB size.
            m_dirty = &m_root;
        }

        m_mode = kOpen;
        return set_status(flush_to_disk());
    }
    return Status::ok();
}

auto Pager::rollback_txn() -> Status
{
    CALICODB_EXPECT_TRUE(m_state->use_wal);
    CALICODB_EXPECT_NE(m_mode, kOpen);
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
        purge_state();
        m_state->status = Status::ok();
        m_mode = kOpen;
    } else {
        m_mode = kError;
    }
    return s;
}

auto Pager::purge_state() -> void
{
    // Note that this will leave referenced pages in the cache.
    CacheEntry *victim;
    while ((victim = m_cache.next_victim())) {
        CALICODB_EXPECT_NE(victim, nullptr);
        purge_entry(*victim);
    }
    if (m_dirty) {
        CALICODB_EXPECT_FALSE(m_dirty->prev);
        CALICODB_EXPECT_FALSE(m_dirty->next);
        m_dirty->is_dirty = false;
        m_dirty = nullptr;
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
        set_status(s);
    }
    return s;
}

auto Pager::wal_checkpoint() -> Status
{
    // A checkpoint must immediately follow a commit, so the cache should be clean.
    CALICODB_EXPECT_EQ(m_dirty, nullptr);
    CALICODB_EXPECT_TRUE(m_in_ckpt);
    std::size_t dbsize;

    // Transfer the WAL contents back to the DB. Note that this call will sync the WAL
    // file before it starts transferring any data back.
    CALICODB_TRY(m_wal->checkpoint(*m_file, &dbsize));

    if (dbsize) {
        set_page_count(dbsize);
        CALICODB_TRY(m_env->resize_file(m_filename, dbsize * m_frames.page_size()));
    }
    return m_file->sync();
}

auto Pager::flush_to_disk() -> Status
{
    if (m_state->use_wal) {
        auto *p = m_dirty;
        for (; p; p = p->next) {
            if (p->page_id.value > m_page_count) {
                // This page is past the current end of the file due to a vacuum operation
                // decreasing the page count. Just remove the page from the dirty list. It
                // wouldn't be transferred back to the DB on checkpoint anyway since it is
                // out of bounds.
                p = clean_page(*p);
            } else {
                p->is_dirty = false;
            }
        }
        p = m_dirty;
        m_dirty = nullptr;

        // The DB page count is specified here. This indicates that the writes are part of
        // a commit, which is always the case if this method is called while the WAL is
        // enabled.
        CALICODB_EXPECT_NE(p, nullptr);
        return m_wal->write(p, m_page_count);
    }

    for (auto *p = m_dirty; p; p = clean_page(*p)) {
        CALICODB_TRY(write_page_to_file(*p));
    }
    return m_file->sync();
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
                // This is an error, regardless of what mode the pager is in. Requires a successful
                // rollback and cache purge.
                set_status(s);
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
    if (!m_freelist.is_empty()) {
        return m_freelist.pop(page);
    }

    const auto allocate_upgraded = [&page, this] {
        auto s = acquire(Id::from_index(m_page_count), page);
        if (s.is_ok()) {
            upgrade(page);
        }
        return s;
    };
    CALICODB_TRY(allocate_upgraded());

    // Since this is a fresh page from the end of the file, it could be a pointer map page. If so,
    // it is already blank, so just skip it and allocate another. It'll get filled in as the pages
    // following it are used by the tree layer.
    if (PointerMap::lookup(*this, page.id()) == page.id()) {
        release(std::move(page));
        return allocate_upgraded();
    }
    return Status::ok();
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
        m_page_count = page_id.value;
    } else if (!page_id.is_root()) {
        // Read a page from either the WAL or the DB.
        CALICODB_TRY(cache_entry(page_id, entry));
    } else {
        // Root page always occupies a frame, but is not stored in the cache
        // structure. If the root page is being acquired through this method
        // (not acquire_root()), then it is being used to access the root
        // tree mapping, not the file header.
        entry = &m_root;
    }
    m_frames.ref(*entry, page);
    return Status::ok();
}

auto Pager::destroy(Page page) -> Status
{
    return m_freelist.push(std::move(page));
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

    if (!page.entry()->is_dirty) {
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
            CALICODB_EXPECT_TRUE(p->is_dirty);
            p = p->next;
        }
    }
#endif // NDEBUG
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
    entry.type = PointerMap::Type {*data++};
    entry.back_ptr.value = get_u32(data);
    return entry;
}

auto PointerMap::lookup(const Pager &pager, Id page_id) -> Id
{
    // Root page (1) has no parents, and page 2 is the first pointer map page. If "page_id" is a pointer map
    // page, "page_id" will be returned.
    if (page_id.value < kFirstMapPage) {
        return Id::null();
    }
    const auto inc = pager.page_size() / kEntrySize + 1;
    const auto idx = (page_id.value - kFirstMapPage) / inc;
    return Id(idx * inc + kFirstMapPage);
}

auto PointerMap::read_entry(Pager &pager, Id page_id, Entry &out) -> Status
{
    const auto mid = lookup(pager, page_id);
    CALICODB_EXPECT_LE(kFirstMapPage, mid.value);
    CALICODB_EXPECT_NE(mid, page_id);

    const auto offset = entry_offset(mid, page_id);
    CALICODB_EXPECT_LE(offset + kEntrySize, pager.page_size());

    Page map;
    CALICODB_TRY(pager.acquire(mid, map));
    out = decode_entry(map.data() + offset);
    pager.release(std::move(map));
    return Status::ok();
}

auto PointerMap::write_entry(Pager &pager, Id page_id, Entry entry) -> Status
{
    const auto mid = lookup(pager, page_id);
    CALICODB_EXPECT_LE(kFirstMapPage, mid.value);
    CALICODB_EXPECT_NE(mid, page_id);

    const auto offset = entry_offset(mid, page_id);
    CALICODB_EXPECT_LE(offset + kEntrySize, pager.page_size());

    Page map;
    CALICODB_TRY(pager.acquire(mid, map));
    const auto [back_ptr, type] = decode_entry(map.data() + offset);
    if (entry.back_ptr != back_ptr || entry.type != type) {
        if (!map.is_writable()) {
            pager.upgrade(map);
        }
        auto data = map.data() + offset;
        *data++ = entry.type;
        put_u32(data, entry.back_ptr.value);
    }
    pager.release(std::move(map));
    return Status::ok();
}

Freelist::Freelist(Pager &pager, Id &head)
    : m_pager {&pager},
      m_head {&head}
{
}

[[nodiscard]] auto Freelist::is_empty() const -> bool
{
    return m_head->is_null();
}

auto Freelist::pop(Page &page) -> Status
{
    if (!m_head->is_null()) {
        CALICODB_TRY(m_pager->acquire(*m_head, page));
        m_pager->upgrade(page);
        *m_head = read_next_id(page);

        if (!m_head->is_null()) {
            // Only clear the back pointer for the new freelist head. Callers must make sure to update the returned
            // node's back pointer at some point.
            const PointerMap::Entry entry = {Id::null(), PointerMap::kFreelistLink};
            CALICODB_TRY(PointerMap::write_entry(*m_pager, *m_head, entry));
        }
        return Status::ok();
    }
    return Status::not_supported("free list is empty");
}

auto Freelist::push(Page page) -> Status
{
    CALICODB_EXPECT_FALSE(page.id().is_root());
    write_next_id(page, *m_head);

    // Write the parent of the old head, if it exists.
    PointerMap::Entry entry = {page.id(), PointerMap::kFreelistLink};
    if (!m_head->is_null()) {
        CALICODB_TRY(PointerMap::write_entry(*m_pager, *m_head, entry));
    }
    // Clear the parent of the new head.
    entry.back_ptr = Id::null();
    CALICODB_TRY(PointerMap::write_entry(*m_pager, page.id(), entry));

    *m_head = page.id();
    m_pager->release(std::move(page));
    return Status::ok();
}

} // namespace calicodb
