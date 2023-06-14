// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "bufmgr.h"
#include "calicodb/env.h"
#include "calicodb/options.h"
#include "encoding.h"
#include "header.h"

namespace calicodb
{

Bufmgr::Bufmgr(std::size_t frame_count)
    : m_buffer(new (std::align_val_t{kPageSize}) char[kPageSize * frame_count]),
      m_frame_count(frame_count)
{
    for (std::size_t i = 1; i < m_frame_count; ++i) {
        m_available.emplace_back(buffer_slot(i));
    }
    m_root.page_id = Id::root();
    m_root.page = buffer_slot(0);
}

Bufmgr::~Bufmgr()
{
    operator delete[] (m_buffer, std::align_val_t{kPageSize});
}

auto Bufmgr::query(Id page_id) -> PageRef *
{
    auto itr = m_map.find(page_id);
    if (itr == end(m_map)) {
        return nullptr;
    }
    return &*itr->second;
}

auto Bufmgr::get(Id page_id) -> PageRef *
{
    CALICODB_EXPECT_FALSE(page_id.is_root());
    auto itr = m_map.find(page_id);
    if (itr == end(m_map)) {
        ++cache_misses;
        return nullptr;
    }
    ++cache_hits;
    m_list.splice(end(m_list), m_list, itr->second);
    return &*itr->second;
}

auto Bufmgr::alloc(Id page_id) -> PageRef *
{
    // The root page is already in a buffer slot. Use root() to get a reference.
    CALICODB_EXPECT_FALSE(page_id.is_root());
    CALICODB_EXPECT_EQ(query(page_id), nullptr);
    auto [itr, _] = m_map.emplace(
        page_id, m_list.emplace(end(m_list)));
    itr->second->page_id = page_id;
    pin(*itr->second);
    return &*itr->second;
}

auto Bufmgr::erase(Id page_id) -> bool
{
    const auto itr = m_map.find(page_id);
    if (itr == end(m_map)) {
        return false;
    }
    // Root page is not stored in the cache.
    CALICODB_EXPECT_FALSE(page_id.is_root());
    CALICODB_EXPECT_EQ(0, itr->second->refcount);
    unpin(*itr->second);
    m_list.erase(itr->second);
    m_map.erase(itr);
    return true;
}

auto Bufmgr::next_victim() -> PageRef *
{
    // NOTE: If this method is being called repeatedly (i.e. to evict all cached pages),
    // then there shouldn't be any outstanding references. The first page in the list
    // should be returned each time this method is called in that case.
    for (auto &ref : m_list) {
        if (ref.refcount == 0) {
            return &ref;
        }
    }
    return nullptr;
}

auto Bufmgr::pin(PageRef &ref) -> void
{
    CALICODB_EXPECT_FALSE(ref.page_id.is_null());
    CALICODB_EXPECT_FALSE(ref.page_id.is_root());
    CALICODB_EXPECT_FALSE(m_available.empty());
    CALICODB_EXPECT_EQ(ref.refcount, 0);

    ref.page = m_available.back();
    m_available.pop_back();
}

auto Bufmgr::unpin(PageRef &ref) -> void
{
    CALICODB_EXPECT_FALSE(ref.page_id.is_null());
    CALICODB_EXPECT_FALSE(ref.page_id.is_root());
    CALICODB_EXPECT_EQ(ref.refcount, 0);

    // The pointer put back into the available pool must (a) not belong to the root
    // page, and (b) point to the start of a page in the buffer.
    CALICODB_EXPECT_GE(ref.page, m_buffer + kPageSize);
    CALICODB_EXPECT_LE(ref.page, m_buffer + (m_frame_count - 1) * kPageSize);
    CALICODB_EXPECT_EQ(reinterpret_cast<std::uintptr_t>(ref.page) % kPageSize, 0);

    m_available.emplace_back(ref.page);
}

auto Bufmgr::ref(PageRef &ref) -> void
{
    CALICODB_EXPECT_FALSE(ref.page_id.is_null());

    ++ref.refcount;
    ++m_refsum;
}

auto Bufmgr::unref(PageRef &ref) -> void
{
    CALICODB_EXPECT_FALSE(ref.page_id.is_null());
    CALICODB_EXPECT_LT(0, ref.refcount);
    CALICODB_EXPECT_LT(0, m_refsum);

    --ref.refcount;
    --m_refsum;
}

auto Dirtylist::remove(PageRef &ref) -> PageRef *
{
    CALICODB_EXPECT_TRUE(head);
    CALICODB_EXPECT_FALSE(head->prev_dirty);
    CALICODB_EXPECT_TRUE(ref.flag & PageRef::kDirty);
    ref.flag = PageRef::kNormal;

    if (ref.prev_dirty) {
        ref.prev_dirty->next_dirty = ref.next_dirty;
    } else {
        CALICODB_EXPECT_EQ(&ref, head);
        head = ref.next_dirty;
    }
    auto *next = ref.next_dirty;
    if (next) {
        next->prev_dirty = ref.prev_dirty;
    }
    ref.dirty = nullptr;
    ref.prev_dirty = nullptr;
    ref.next_dirty = nullptr;
    return next;
}

auto Dirtylist::add(PageRef &ref) -> void
{
    CALICODB_EXPECT_FALSE(ref.flag & PageRef::kDirty);
    if (head) {
        CALICODB_EXPECT_FALSE(head->prev_dirty);
        head->prev_dirty = &ref;
    }
    ref.flag = PageRef::kDirty;
    ref.dirty = nullptr;
    ref.prev_dirty = nullptr;
    ref.next_dirty = head;
    head = &ref;
}

static auto dirtylist_merge(PageRef &lhs_ref, PageRef &rhs_ref) -> PageRef *
{
    PageRef result;
    auto *tail = &result;
    auto *lhs = &lhs_ref;
    auto *rhs = &rhs_ref;

    for (;;) {
        if (lhs->page_id < rhs->page_id) {
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

auto Dirtylist::sort() -> void
{
#ifndef NDEBUG
    auto *old_head = head;
#endif // NDEBUG

    for (auto *p = head; p; p = p->next_dirty) {
        p->dirty = p->next_dirty;
    }

    static constexpr std::size_t kNSortBuckets = 32;
    auto *in = head;
    PageRef *arr[kNSortBuckets] = {};
    PageRef *ptr;

    while (in) {
        ptr = in;
        in = in->dirty;
        ptr->dirty = nullptr;

        std::size_t i = 0;
        for (; i < kNSortBuckets - 1; ++i) {
            if (arr[i] == nullptr) {
                arr[i] = ptr;
                break;
            } else {
                ptr = dirtylist_merge(*arr[i], *ptr);
                arr[i] = nullptr;
            }
        }
        if (i == kNSortBuckets - 1) {
            arr[i] = dirtylist_merge(*arr[i], *ptr);
        }
    }
    ptr = arr[0];
    for (std::size_t i = 1; i < kNSortBuckets; ++i) {
        if (arr[i] == nullptr) {
            continue;
        }
        ptr = ptr ? dirtylist_merge(*ptr, *arr[i]) : arr[i];
    }
    head = ptr;

#ifndef NDEBUG
    // Make sure the list was sorted correctly.
    for (PageRef *transient = head, *permanent = old_head; transient;) {
        auto *next = transient->dirty;
        if (next != nullptr) {
            CALICODB_EXPECT_LT(transient->page_id, next->page_id);
        }
        transient = next;

        // Traverse the non-transient list as well. It should be the same length.
        CALICODB_EXPECT_EQ(!next, !permanent->next_dirty);
        permanent = permanent->next_dirty;
    }
#endif // NDEBUG
}

} // namespace calicodb
