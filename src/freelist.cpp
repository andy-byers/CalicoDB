// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "freelist.h"
#include "header.h"
#include "pager.h"

namespace calicodb
{

static constexpr size_t kTrunkCapacity = (kPageSize - 2 * sizeof(uint32_t)) / sizeof(uint32_t);

template <class Char>
static auto get_leaf_ptr(Char *base, size_t index) -> Char *
{
    return base + (index + 2) * sizeof(uint32_t);
}

auto Freelist::push(Pager &pager, PageRef *&page) -> Status
{
    CALICODB_EXPECT_NE(nullptr, page);
    if (page->page_id.value <= kFirstMapPage ||
        page->page_id.value > pager.page_count()) {
        pager.release(page);
        return Status::corruption();
    }
    auto &root = pager.get_root();
    PageRef *trunk = nullptr;
    Status s;

    // Page ID of the first freelist trunk page.
    auto free_head = FileHdr::get_freelist_head(root.data);
    if (free_head.value > pager.page_count()) {
        s = Status::corruption();
    } else if (!free_head.is_null()) {
        s = pager.acquire(free_head, trunk);
        if (s.is_ok()) {
            const auto n = get_u32(trunk->data + sizeof(uint32_t));
            if (n < kTrunkCapacity) {
                // trunk has enough room for a new leaf page.
                pager.mark_dirty(*trunk);
                put_u32(trunk->data + sizeof(uint32_t), n + 1);
                put_u32(get_leaf_ptr(trunk->data, n), page->page_id.value);
                s = PointerMap::write_entry(
                    pager, page->page_id, {free_head, PointerMap::kFreelistLeaf});
                goto cleanup;
            } else if (n > kTrunkCapacity) {
                s = Status::corruption();
                goto cleanup;
            }
            // There is a trunk page already, but it didn't have room for another leaf pointer. `page`
            // will be set as the new first trunk page below, so point the old head's back pointer at it.
            s = PointerMap::write_entry(
                pager, free_head, {page->page_id, PointerMap::kFreelistTrunk});
        }
    }
    if (s.is_ok()) {
        // `page` must become a new freelist trunk page. Update the file header to reflect this.
        pager.mark_dirty(root);
        FileHdr::put_freelist_head(root.data, page->page_id);
        // Transform `page` into a blank freelist trunk page that points at what was previously the first
        // trunk page. Only need to modify the first 8 bytes.
        pager.mark_dirty(*page);
        put_u32(page->data, free_head.value);
        put_u32(page->data + sizeof(uint32_t), 0);
        // Point the new head's back pointer at Id::null().
        free_head = page->page_id;
        // Release the page before it gets discarded below.
        pager.release(page);
        s = PointerMap::write_entry(
            pager, free_head, {Id::null(), PointerMap::kFreelistTrunk});
    }

cleanup:
    if (s.is_ok()) {
        pager.mark_dirty(root);
        const auto freelist_len = FileHdr::get_freelist_length(root.data);
        FileHdr::put_freelist_length(root.data, freelist_len + 1);
    }
    pager.release(trunk);
    pager.release(page, Pager::kDiscard);
    return s;
}

// NOTE: The pointer map entry for `id_out` is not updated before this function returns. It is up to
// the caller to call PointerMap::write_entry() when the back pointer and page type are known.
auto Freelist::pop(Pager &pager, Id &id_out) -> Status
{
    auto &root = pager.get_root();
    const auto free_length = FileHdr::get_freelist_length(root.data);
    auto free_head = FileHdr::get_freelist_head(root.data);
    if (free_length == 0) {
        return Status::invalid_argument();
    } else if (free_head.value > pager.page_count()) {
        return Status::corruption();
    }
    PageRef *trunk;
    auto s = pager.acquire(free_head, trunk);
    if (s.is_ok()) {
        const auto n = get_u32(trunk->data + sizeof(uint32_t));
        if (n > kTrunkCapacity) {
            s = Status::corruption();
        } else if (n > 0) {
            pager.mark_dirty(*trunk);
            auto *ptr = get_leaf_ptr(trunk->data, n - 1);
            id_out.value = get_u32(ptr);
            put_u32(ptr, 0);
            put_u32(trunk->data + sizeof(uint32_t), n - 1);
        } else {
            id_out = free_head;
            free_head.value = get_u32(trunk->data);
            pager.mark_dirty(root);
            FileHdr::put_freelist_head(root.data, free_head);
            if (!free_head.is_null()) {
                s = PointerMap::write_entry(
                    pager, free_head, {Id::null(), PointerMap::kFreelistTrunk});
            }
        }
        pager.release(trunk);
        if (s.is_ok()) {
            pager.mark_dirty(root);
            FileHdr::put_freelist_length(root.data, free_length - 1);
        }
    }
    return s;
}

auto Freelist::assert_state(Pager &pager) -> bool
{
    PageRef *head = nullptr;
    auto &root = pager.get_root();

    auto free_head = FileHdr::get_freelist_head(root.data);
    CALICODB_EXPECT_LE(free_head.value, pager.page_count());
    CALICODB_EXPECT_TRUE(free_head.is_null() || free_head.value > kFirstMapPage);

    Status s;
    Id last_id;
    while (!free_head.is_null()) {
        s = pager.acquire(free_head, head);
        CALICODB_EXPECT_TRUE(s.is_ok());
        const auto n = get_u32(head->data + sizeof(uint32_t));
        CALICODB_EXPECT_LE(n, kTrunkCapacity);

        PointerMap::Entry entry;
        s = PointerMap::read_entry(pager, free_head, entry);
        CALICODB_EXPECT_TRUE(s.is_ok());
        CALICODB_EXPECT_EQ(entry.back_ptr, last_id);
        CALICODB_EXPECT_EQ(entry.type, PointerMap::kFreelistTrunk);

        for (size_t i = 0; i < n; ++i) {
            const Id leaf_id(get_u32(get_leaf_ptr(head->data, i)));
            CALICODB_EXPECT_FALSE(leaf_id.is_null());
            CALICODB_EXPECT_LE(leaf_id.value, pager.page_count());
            s = PointerMap::read_entry(pager, leaf_id, entry);
            CALICODB_EXPECT_TRUE(s.is_ok());
            // Leaf back pointers must point to the parent trunk page.
            CALICODB_EXPECT_EQ(entry.back_ptr, free_head);
            CALICODB_EXPECT_EQ(entry.type, PointerMap::kFreelistLeaf);
        }
        last_id = free_head;
        free_head.value = get_u32(head->data);
        pager.release(head);
    }
    return true;
}

} // namespace calicodb
