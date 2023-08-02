// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "bufmgr.h"
#include "calicodb/env.h"
#include "calicodb/options.h"
#include "encoding.h"
#include "header.h"
#include "stat.h"

namespace calicodb
{

template <class Entry>
static auto list_add_between(Entry &entry, Entry &prev, Entry &next) -> void
{
    next.prev = &entry;
    entry.next = &next;
    entry.prev = &prev;
    prev.next = &entry;
}

template <class Entry>
static auto list_add_head(Entry &ref, Entry &head) -> void
{
    list_add_between(ref, head, *head.next);
}

template <class Entry>
static auto list_add_tail(Entry &entry, Entry &head) -> void
{
    list_add_between(entry, *head.prev, head);
}

template <class Entry>
static auto list_remove(Entry &entry) -> void
{
    entry.next->prev = entry.prev;
    entry.prev->next = entry.next;
}

template <class Entry>
[[nodiscard]] static auto list_is_empty(const Entry &entry) -> bool
{
    return &entry == entry.next;
}

Bufmgr::Bufmgr(std::size_t min_buffers, Stat &stat)
    : m_root(alloc_page()),
      m_min_buffers(min_buffers),
      m_stat(&stat)
{
    // We don't call alloc_page() for the dummy list heads, so the circular
    // connections must be initialized manually.
    m_in_use.prev = &m_in_use;
    m_in_use.next = &m_in_use;
    m_lru.prev = &m_lru;
    m_lru.next = &m_lru;

    if (m_root) {
        m_root->page_id = Id::root();
    }

    for (m_num_buffers = 0; m_num_buffers < m_min_buffers; ++m_num_buffers) {
        if (auto *ref = alloc_page()) {
            list_add_tail(*ref, m_lru);
        } else {
            break;
        }
    }

    m_map.reserve(min_buffers);
}

Bufmgr::~Bufmgr()
{
    free_page(m_root);

    // The pager should have released any referenced pages before the buffer manager
    // is destroyed.
    CALICODB_EXPECT_TRUE(list_is_empty(m_in_use));

    for (auto *p = m_lru.next; p != &m_lru;) {
        auto *ptr = p;
        p = p->next;
        free_page(ptr);
    }
}

auto Bufmgr::query(Id page_id) const -> PageRef *
{
    auto itr = m_map.find(page_id);
    if (itr == end(m_map)) {
        return nullptr;
    }
    return &*itr->second;
}

auto Bufmgr::lookup(Id page_id) -> PageRef *
{
    CALICODB_EXPECT_FALSE(page_id.is_root());
    auto itr = m_map.find(page_id);
    if (itr == end(m_map)) {
        ++m_stat->counters[Stat::kCacheMisses];
        return nullptr;
    }
    ++m_stat->counters[Stat::kCacheHits];
    if (itr->second->refs == 0) {
        list_remove(*itr->second);
        list_add_head(*itr->second, m_lru);
    }
    return &*itr->second;
}

auto Bufmgr::next_victim() -> PageRef *
{
    return list_is_empty(m_lru) ? nullptr : m_lru.prev;
}

auto Bufmgr::allocate() -> PageRef *
{
    auto *ref = alloc_page();
    list_add_tail(*ref, m_lru);
    ++m_num_buffers;
    return ref;
}

auto Bufmgr::register_page(PageRef &page) -> void
{
    if (Id::root() < page.page_id) {
        CALICODB_EXPECT_TRUE(query(page.page_id) == nullptr);
        CALICODB_EXPECT_FALSE(page.get_flag(PageRef::kCached));
        m_map.insert_or_assign(page.page_id, &page);
        page.set_flag(PageRef::kCached);
    }
}

auto Bufmgr::erase(Id page_id) -> bool
{
    const auto itr = m_map.find(page_id);
    if (itr == end(m_map)) {
        return false;
    }
    CALICODB_EXPECT_LT(Id::root(), page_id);
    auto &ref = itr->second;
    CALICODB_EXPECT_TRUE(ref->get_flag(PageRef::kCached));
    list_remove(*ref);
    list_add_tail(*ref, m_lru);
    ref->clear_flag(PageRef::kCached);
    m_map.erase(itr);
    return true;
}

auto Bufmgr::purge() -> void
{
    CALICODB_EXPECT_TRUE(list_is_empty(m_in_use));
    CALICODB_EXPECT_EQ(m_refsum, 0);
    for (const auto &[page_id, ref] : m_map) {
        ref->flag = PageRef::kNormal;
    }
    m_map.clear();
}

auto Bufmgr::ref(PageRef &ref) -> void
{
    ++ref.refs;
    ++m_refsum;
    if (ref.refs == 1) {
        list_remove(ref);
        list_add_head(ref, m_in_use);
    }
}

auto Bufmgr::unref(PageRef &ref) -> void
{
    CALICODB_EXPECT_GT(ref.refs, 0);
    CALICODB_EXPECT_GT(m_refsum, 0);

    --ref.refs;
    --m_refsum;
    if (ref.refs == 0) {
        list_remove(ref);
        list_add_head(ref, m_lru);
    }
}

auto Bufmgr::shrink_to_fit() -> void
{
    CALICODB_EXPECT_EQ(m_refsum, 0);
    while (m_num_buffers > m_min_buffers) {
        auto *lru = m_lru.prev;
        m_map.erase(lru->page_id);
        list_remove(*lru);
        delete lru;
    }
}

auto Bufmgr::assert_state() const -> bool
{
    // Make sure the refcounts add up to the "refsum".
    U32 refsum = 0;
    for (auto p = m_in_use.next; p != &m_in_use; p = p->next) {
        const auto itr = m_map.find(p->page_id);
        CALICODB_EXPECT_NE(itr, end(m_map));
        CALICODB_EXPECT_EQ(p, itr->second);
        CALICODB_EXPECT_GT(p->refs, 0);
        refsum += p->refs;
        (void)itr;
    }
    for (auto p = m_lru.next; p != &m_lru; p = p->next) {
        if (p->get_flag(PageRef::kDirty)) {
            // Pages that are dirty must remain in the cache. Otherwise, we risk having 2 dirty
            // copies of the same page in the dirtylist at the same time.
            CALICODB_EXPECT_TRUE(p->get_flag(PageRef::kCached));
        }
        if (p->get_flag(PageRef::kCached)) {
            const auto itr = m_map.find(p->page_id);
            if (itr != end(m_map)) {
                CALICODB_EXPECT_EQ(p, itr->second);
            }
        }
        CALICODB_EXPECT_EQ(p->refs, 0);
    }
    return refsum == m_refsum;
}

auto Dirtylist::is_empty() const -> bool
{
    return list_is_empty(m_head);
}

auto Dirtylist::remove(PageRef &ref) -> DirtyHdr *
{
    CALICODB_EXPECT_FALSE(list_is_empty(m_head));
    CALICODB_EXPECT_TRUE(ref.get_flag(PageRef::kDirty));
    CALICODB_EXPECT_TRUE(TEST_contains(ref));
    auto *hdr = ref.get_dirty_hdr();
    list_remove(*hdr);
    ref.clear_flag(PageRef::kDirty);
    return hdr->next;
}

auto Dirtylist::add(PageRef &ref) -> void
{
    CALICODB_EXPECT_FALSE(ref.get_flag(PageRef::kDirty));
    CALICODB_EXPECT_FALSE(TEST_contains(ref));
    list_add_head(*ref.get_dirty_hdr(), m_head);
    ref.set_flag(PageRef::kDirty);
}

static auto dirtylist_merge(DirtyHdr *lhs, DirtyHdr *rhs) -> DirtyHdr *
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

// NOTE: Sorting routine is from SQLite (src/pcache.c).
auto Dirtylist::sort() -> DirtyHdr *
{
#ifndef NDEBUG
    auto *old_head = begin();
#endif // NDEBUG

    CALICODB_EXPECT_FALSE(is_empty());
    for (auto *p = begin(); p != end(); p = p->next) {
        p->dirty = p->next == end() ? nullptr : p->next;
    }

    static constexpr std::size_t kNSortBuckets = 32;
    auto *in = begin();
    DirtyHdr *arr[kNSortBuckets] = {};
    DirtyHdr *ptr;

    while (in) {
        ptr = in;
        in = in->dirty;
        ptr->dirty = nullptr;

        std::size_t i = 0;
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
    for (std::size_t i = 1; i < kNSortBuckets; ++i) {
        if (arr[i]) {
            ptr = ptr ? dirtylist_merge(ptr, arr[i]) : arr[i];
        }
    }
    m_head.prev = end();
    m_head.next = end();

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
        CALICODB_EXPECT_EQ(!next, permanent->next == end());
        permanent = permanent->next;
    }
#endif // NDEBUG
    return ptr;
}

auto Dirtylist::TEST_contains(const PageRef &ref) const -> bool
{
    auto found = false;
    for (const auto *p = begin(); p != end(); p = p->next) {
        CALICODB_EXPECT_TRUE(p->next == end() ||
                             p->next->prev == p);
        if (p->get_page_ref()->page_id == ref.page_id) {
            CALICODB_EXPECT_EQ(p, ref.get_dirty_hdr());
            CALICODB_EXPECT_FALSE(found);
            found = true;
        }
    }
    return found;
}

} // namespace calicodb
