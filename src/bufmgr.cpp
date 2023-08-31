// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "bufmgr.h"
#include "calicodb/env.h"
#include "encoding.h"
#include "stat.h"

namespace calicodb
{

Bufmgr::Bufmgr(Stat &stat)
    : m_root(nullptr),
      m_min_buffers(0),
      m_num_buffers(0),
      m_stat(&stat)
{
    // We don't call alloc_page() for the dummy list heads, so the circular
    // connections must be initialized manually.
    IntrusiveList::initialize(m_in_use);
    IntrusiveList::initialize(m_lru);
}

Bufmgr::~Bufmgr()
{
    PageRef::free(m_root);

    // The pager should have released any referenced pages before the buffer manager
    // is destroyed.
    CALICODB_EXPECT_TRUE(IntrusiveList::is_empty(m_in_use));

    for (auto *p = m_lru.next_entry; p != &m_lru;) {
        auto *ptr = p;
        p = p->next_entry;
        PageRef::free(ptr);
    }
}

auto Bufmgr::preallocate(size_t min_buffers) -> int
{
    CALICODB_EXPECT_EQ(m_num_buffers, 0);
    for (; m_num_buffers < min_buffers; ++m_num_buffers) {
        if (auto *ref = PageRef::alloc()) {
            IntrusiveList::add_tail(*ref, m_lru);
        } else {
            return -1;
        }
    }
    m_root = PageRef::alloc();
    if (!m_root || m_table.preallocate(min_buffers)) {
        return -1;
    }
    m_root->page_id = Id::root();
    m_min_buffers = min_buffers;
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
        ++m_stat->counters[Stat::kCacheMisses];
        return nullptr;
    }
    ++m_stat->counters[Stat::kCacheHits];
    if (ref->refs == 0) {
        IntrusiveList::remove(*ref);
        IntrusiveList::add_head(*ref, m_lru);
    }
    return ref;
}

auto Bufmgr::next_victim() -> PageRef *
{
    return IntrusiveList::is_empty(m_lru) ? nullptr : m_lru.prev_entry;
}

auto Bufmgr::allocate() -> PageRef *
{
    auto *ref = PageRef::alloc();
    if (ref) {
        IntrusiveList::add_tail(*ref, m_lru);
        ++m_num_buffers;
    }
    return ref;
}

auto Bufmgr::register_page(PageRef &page) -> void
{
    if (Id::root() < page.page_id) {
        CALICODB_EXPECT_TRUE(query(page.page_id) == nullptr);
        CALICODB_EXPECT_FALSE(page.get_flag(PageRef::kCached));
        m_table.insert(&page);
        page.set_flag(PageRef::kCached);
    }
}

auto Bufmgr::erase(PageRef &ref) -> void
{
    if (ref.get_flag(PageRef::kCached)) {
        ref.clear_flag(PageRef::kCached);
        m_table.remove(ref.key());
        IntrusiveList::remove(ref);
        IntrusiveList::add_tail(ref, m_lru);
    }
}

auto Bufmgr::purge() -> void
{
    CALICODB_EXPECT_TRUE(IntrusiveList::is_empty(m_in_use));
    CALICODB_EXPECT_EQ(m_refsum, 0);
    for (auto *ref = m_lru.next_entry;
         ref != &m_lru; ref = ref->next_entry) {
        ref->flag = PageRef::kNormal;
    }
    m_table.clear();
}

auto Bufmgr::ref(PageRef &ref) -> void
{
    ++ref.refs;
    ++m_refsum;
    if (ref.refs == 1) {
        IntrusiveList::remove(ref);
        IntrusiveList::add_head(ref, m_in_use);
    }
}

auto Bufmgr::unref(PageRef &ref) -> void
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

auto Bufmgr::shrink_to_fit() -> void
{
    CALICODB_EXPECT_EQ(m_refsum, 0);
    while (m_num_buffers > m_min_buffers) {
        auto *lru = m_lru.prev_entry;
        m_table.remove(lru->key());
        IntrusiveList::remove(*lru);
        PageRef::free(lru);
        --m_num_buffers;
    }
}

auto Bufmgr::assert_state() const -> bool
{
    // Make sure the refcounts add up to the "refsum".
    uint32_t refsum = 0;
    for (auto p = m_in_use.next_entry; p != &m_in_use; p = p->next_entry) {
        const auto *ref = m_table.lookup(p->key());
        CALICODB_EXPECT_NE(ref, nullptr);
        CALICODB_EXPECT_EQ(p, ref);
        CALICODB_EXPECT_GT(p->refs, 0);
        refsum += p->refs;
        (void)ref;
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

auto Dirtylist::add(PageRef &ref) -> void
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
