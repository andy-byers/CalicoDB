// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "bufmgr.h"
#include "calicodb/env.h"
#include "encoding.h"

namespace calicodb
{

Bufmgr::Bufmgr(size_t min_buffers, Stats &stat)
    : m_stat(&stat),
      m_min_buffers(min_buffers)
{
    CALICODB_EXPECT_GE(min_buffers, kMinFrameCount);
    free_buffers();
}

Bufmgr::~Bufmgr()
{
    // The pager should have released any referenced pages before the buffer manager
    // is destroyed.
    free_buffers();
}

void Bufmgr::free_buffers()
{
    shrink_to_fit();
    m_backing.reset();
    m_metadata.reset();
    IntrusiveList::initialize(m_in_use);
    IntrusiveList::initialize(m_lru);
    m_num_buffers = 0;
    m_root = nullptr;
}

auto Bufmgr::reallocate(size_t page_size) -> int
{
    free_buffers();

    const auto buffer_size = page_size + kSpilloverLen;
    const auto num_buffers = m_min_buffers + 1;
    if (m_backing.realloc(buffer_size * num_buffers)) {
        return -1;
    }
    if (m_metadata.realloc(num_buffers)) {
        return -1;
    }
    // The hash table is only allocated once. The total number of bytes allotted to the
    // page cache has not changed, just the page size.
    if (m_table.m_capacity == 0 && m_table.allocate(m_min_buffers)) {
        return -1;
    }
    for (size_t i = 0; i < num_buffers; ++i) {
        PageRef::init(m_metadata[i], &m_backing[buffer_size * i]);
        IntrusiveList::add_tail(m_metadata[i], m_lru);
    }
    m_num_buffers = m_min_buffers;
    // Reserve the first page buffer for page 1.
    m_root = m_metadata.data();
    m_root->page_id = Id::root();
    IntrusiveList::remove(*m_root);
    std::memset(m_root->data, 0, page_size);
    return 0;
}

auto Bufmgr::query(Id page_id) const -> PageRef *
{
    return m_table.lookup(page_id.value);
}

auto Bufmgr::lookup(Id page_id) -> PageRef *
{
    CALICODB_EXPECT_FALSE(page_id.is_root());
    auto *ref = m_table.lookup(page_id.value);
    if (ref == nullptr) {
        ++m_stat->cache_misses;
        return nullptr;
    }
    ++m_stat->cache_hits;
    if (ref->refs == 0) {
        // Make ref the most-recently-used element.
        IntrusiveList::remove(*ref);
        IntrusiveList::add_head(*ref, m_lru);
    }
    return ref;
}

auto Bufmgr::next_victim() -> PageRef *
{
    return IntrusiveList::is_empty(m_lru) ? nullptr : m_lru.prev_entry;
}

auto Bufmgr::allocate(size_t page_size) -> PageRef *
{
    auto *ref = PageRef::alloc(page_size);
    if (ref) {
        if (m_extra) {
            ref->next_extra = m_extra;
        }
        m_extra = ref;
        IntrusiveList::add_tail(*ref, m_lru);
        ++m_num_buffers;
    }
    return ref;
}

void Bufmgr::register_page(PageRef &page)
{
    if (Id::root() < page.page_id) {
        CALICODB_EXPECT_EQ(query(page.page_id), nullptr);
        CALICODB_EXPECT_FALSE(page.get_flag(PageRef::kCached));
        m_table.insert(&page);
        page.set_flag(PageRef::kCached);
    }
}

void Bufmgr::erase(PageRef &ref)
{
    if (Id::root() < ref.page_id) {
        if (ref.get_flag(PageRef::kCached)) {
            ref.clear_flag(PageRef::kCached);
            m_table.remove(ref.key());
        }
        IntrusiveList::remove(ref);
        IntrusiveList::add_tail(ref, m_lru);
    }
}

void Bufmgr::purge()
{
    CALICODB_EXPECT_TRUE(IntrusiveList::is_empty(m_in_use));
    CALICODB_EXPECT_EQ(m_refsum, 0);
    for (auto *ref = m_lru.next_entry;
         ref != &m_lru;
         ref = ref->next_entry) {
        ref->flag = PageRef::kNormal;
    }
    m_table.clear();
}

void Bufmgr::ref(PageRef &ref)
{
    ++ref.refs;
    ++m_refsum;
    if (ref.refs == 1) {
        IntrusiveList::remove(ref);
        IntrusiveList::add_head(ref, m_in_use);
    }
}

void Bufmgr::unref(PageRef &ref)
{
    CALICODB_EXPECT_GT(ref.refs, 0);
    CALICODB_EXPECT_GT(m_refsum, 0);

    --ref.refs;
    --m_refsum;
    if (ref.refs == 0) {
        IntrusiveList::remove(ref);
        IntrusiveList::add_head(ref, m_lru);
    }
}

void Bufmgr::shrink_to_fit()
{
    CALICODB_EXPECT_EQ(m_refsum, 0);
    for (auto *ref = m_extra; ref;) {
        if (ref->get_flag(PageRef::kCached)) {
            m_table.remove(ref->key());
        }
        --m_num_buffers;
        IntrusiveList::remove(*ref);
        auto *next = ref->next_extra;
        PageRef::free(ref);
        ref = next;
    }
    m_extra = nullptr;
}

auto Bufmgr::assert_state() const -> bool
{
#ifndef NDEBUG
    // Make sure the refcounts add up to the "refsum".
    uint32_t refsum = 0;
    for (auto p = m_in_use.next_entry; p != &m_in_use; p = p->next_entry) {
        [[maybe_unused]] const auto *ref = m_table.lookup(p->key());
        CALICODB_EXPECT_NE(ref, nullptr);
        // If this check fails, then look at usage of get_unused_page(). May have been replacing
        // a page with new content, i.e. what happens in Tree::redistribute_cells().
        CALICODB_EXPECT_EQ(p, ref);
        CALICODB_EXPECT_GT(p->refs, 0);
        refsum += p->refs;
    }
    for (auto p = m_lru.next_entry; p != &m_lru; p = p->next_entry) {
        if (p->get_flag(PageRef::kDirty)) {
            // Pages that are dirty must remain in the cache. Otherwise, we risk having 2 dirty
            // copies of the same page in the dirtylist at the same time.
            CALICODB_EXPECT_TRUE(p->get_flag(PageRef::kCached));
        }
        if (p->get_flag(PageRef::kCached)) {
            const auto *ref = m_table.lookup(p->key());
            if (ref != nullptr) {
                CALICODB_EXPECT_EQ(p, ref);
            }
        }
        CALICODB_EXPECT_EQ(p->refs, 0);
    }
    return refsum == m_refsum;
#else
    return true;
#endif // NDEBUG
}

auto Bufmgr::PageTable::allocate(size_t min_buffers) -> int
{
    CALICODB_EXPECT_EQ(m_capacity, 0);
    uint32_t capacity = 4;
    while (capacity < min_buffers) {
        capacity *= 2;
    }
    const auto table_size = capacity * sizeof(PageRef *);
    if (auto *table = static_cast<PageRef **>(
            Mem::allocate(table_size))) {
        m_capacity = capacity;
        m_table = table;
        clear();
        return 0;
    }
    return -1;
}

void Bufmgr::PageTable::insert(PageRef *ref)
{
    auto **ptr = find_pointer(ref->key());
    CALICODB_EXPECT_NE(ptr, nullptr);
    CALICODB_EXPECT_EQ(*ptr, nullptr);
    ref->next_hash = nullptr;
    *ptr = ref;
    ++m_length;
}

auto Bufmgr::PageTable::remove(uint32_t key) -> PageRef *
{
    auto **ptr = find_pointer(key);
    auto *result = *ptr;
    if (result != nullptr) {
        *ptr = result->next_hash;
        --m_length;
    }
    return result;
}

auto Dirtylist::is_empty() const -> bool
{
    return IntrusiveList::is_empty(m_head);
}

auto Dirtylist::remove(PageRef &ref) -> DirtyHdr *
{
    CALICODB_EXPECT_TRUE(TEST_contains(ref));
    CALICODB_EXPECT_TRUE(ref.get_flag(PageRef::kDirty));
    // NOTE: ref.dirty_hdr.next_entry is still valid after this call (IntrusiveList::remove() does not
    //       reinitialize the entry it removes from the list).
    IntrusiveList::remove(ref.dirty_hdr);
    ref.clear_flag(PageRef::kDirty);
    return ref.dirty_hdr.next_entry;
}

void Dirtylist::add(PageRef &ref)
{
    CALICODB_EXPECT_FALSE(TEST_contains(ref));
    CALICODB_EXPECT_FALSE(ref.get_flag(PageRef::kDirty));
    IntrusiveList::add_head(ref.dirty_hdr, m_head);
    ref.set_flag(PageRef::kDirty);
}

namespace
{

auto dirtylist_merge(DirtyHdr *lhs, DirtyHdr *rhs) -> DirtyHdr *
{
    DirtyHdr result = {};
    auto *tail = &result;
    CALICODB_EXPECT_TRUE(lhs && rhs);
    for (;;) {
        if (lhs->get_page_ref()->page_id <
            rhs->get_page_ref()->page_id) {
            tail->dirty = lhs;
            tail = lhs;
            lhs = lhs->dirty;
            if (lhs == nullptr) {
                tail->dirty = rhs;
                break;
            }
        } else {
            tail->dirty = rhs;
            tail = rhs;
            rhs = rhs->dirty;
            if (rhs == nullptr) {
                tail->dirty = lhs;
                break;
            }
        }
    }
    return result.dirty;
}

} // namespace

// NOTE: Sorting routine is from SQLite (src/pcache.c).
auto Dirtylist::sort() -> DirtyHdr *
{
#ifndef NDEBUG
    auto *old_head = begin();
#endif // NDEBUG

    CALICODB_EXPECT_FALSE(is_empty());
    for (auto *p = begin(); p != end(); p = p->next_entry) {
        p->dirty = p->next_entry == end() ? nullptr : p->next_entry;
    }

    static constexpr size_t kNSortBuckets = 32;
    auto *in = begin();
    DirtyHdr *arr[kNSortBuckets] = {};
    DirtyHdr *ptr;

    while (in) {
        ptr = in;
        in = in->dirty;
        ptr->dirty = nullptr;

        size_t i = 0;
        for (; i < kNSortBuckets - 1; ++i) {
            if (arr[i]) {
                ptr = dirtylist_merge(arr[i], ptr);
                arr[i] = nullptr;
            } else {
                arr[i] = ptr;
                break;
            }
        }
        if (i == kNSortBuckets - 1) {
            arr[i] = dirtylist_merge(arr[i], ptr);
        }
    }
    ptr = arr[0];
    for (size_t i = 1; i < kNSortBuckets; ++i) {
        if (arr[i]) {
            ptr = ptr ? dirtylist_merge(ptr, arr[i]) : arr[i];
        }
    }
    m_head.prev_entry = end();
    m_head.next_entry = end();

#ifndef NDEBUG
    // Make sure the list was sorted correctly.
    for (DirtyHdr *transient = ptr, *permanent = old_head; transient;) {
        CALICODB_EXPECT_NE(permanent, end());
        auto *next = transient->dirty;
        if (next != nullptr) {
            CALICODB_EXPECT_LT(transient->get_page_ref()->page_id,
                               next->get_page_ref()->page_id);
        }
        transient = next;

        // Traverse the non-transient list as well. It should be the same length.
        CALICODB_EXPECT_EQ(!next, permanent->next_entry == end());
        permanent = permanent->next_entry;
    }
#endif // NDEBUG
    return ptr;
}

auto Dirtylist::TEST_contains(const PageRef &ref) const -> bool
{
    auto found = false;
    for (const auto *p = begin(); p != end(); p = p->next_entry) {
        CALICODB_EXPECT_TRUE(p->next_entry == end() ||
                             p->next_entry->prev_entry == p);
        if (p->get_page_ref()->page_id == ref.page_id) {
            CALICODB_EXPECT_EQ(p, &ref.dirty_hdr);
            CALICODB_EXPECT_FALSE(found);
            found = true;
        }
    }
    return found;
}

} // namespace calicodb
