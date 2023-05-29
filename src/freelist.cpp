// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "freelist.h"
#include "pager.h"
#include "scope_guard.h"

namespace calicodb
{

static constexpr std::size_t kTrunkCapacity = (kPageSize - 2 * sizeof(U32)) / sizeof(U32);

template <class Char>
static auto get_leaf_ptr(Char *base, std::size_t index) -> Char *
{
    return base + (index + 2) * sizeof(U32);
}

auto Freelist::is_empty(Pager &pager) -> bool
{
    auto root = pager.acquire_root();
    const auto result = FileHeader::get_freelist_head(
                            root.constant_ptr())
                            .is_null();
    pager.release(std::move(root));
    return result;
}

auto Freelist::push(Pager &pager, Page page) -> Status
{
    if (page.id().value < kFirstMapPage || page.id().value > pager.page_count()) {
        return Status::corruption();
    }
    Page trunk;
    auto root = pager.acquire_root();
    ScopeGuard guard = [&pager, &trunk, &root] {
        pager.release(std::move(root));
        pager.release(std::move(trunk));
    };

    // Page ID of the first freelist trunk page.
    auto free_head = FileHeader::get_freelist_head(root.constant_ptr());
    if (free_head.value > pager.page_count()) {
        return Status::corruption();
    }
    if (!free_head.is_null()) {
        auto s = pager.acquire(free_head, trunk);
        if (s.is_ok()) {
            const auto n = get_u32(trunk.constant_ptr() + sizeof(U32));
            if (n < kTrunkCapacity) {
                pager.mark_dirty(trunk);
                put_u32(trunk.mutable_ptr() + sizeof(U32), n + 1);
                put_u32(get_leaf_ptr(trunk.mutable_ptr(), n), page.id().value);
                return PointerMap::write_entry(
                    pager, page.id(), {free_head, PointerMap::kFreelistLeaf});
            } else if (n > kTrunkCapacity) {
                return Status::corruption();
            }
        }
        // There is a trunk page already, but it didn't have room for another leaf pointer. `page`
        // will be set as the new first trunk page, so point the old head's back pointer at it.
        s = PointerMap::write_entry(
            pager, free_head, {page.id(), PointerMap::kFreelistTrunk});
        if (!s.is_ok()) {
            return s;
        }
    }
    // `page` must become a new freelist trunk page. Update the file header to reflect this.
    pager.mark_dirty(root);
    FileHeader::put_freelist_head(root.mutable_ptr(), page.id());
    // Transform `page` into a blank freelist trunk page that points at what was previously the first
    // trunk page. Only need to modify the first 8 bytes.
    pager.mark_dirty(page);
    put_u32(page.mutable_ptr(), free_head.value);
    put_u32(page.mutable_ptr() + sizeof(U32), 0);
    // Point the new head's back pointer at Id::null().
    free_head = page.id();
    pager.release(std::move(page));
    return PointerMap::write_entry(
        pager, free_head, {Id::null(), PointerMap::kFreelistTrunk});
}

// NOTE: The pointer map entry for `id_out` is not updated before this function returns. It is up to
// the caller to call PointerMap::write_entry() when the back pointer and page type are known.
auto Freelist::pop(Pager &pager, Id &id_out) -> Status
{
    Page trunk;
    auto root = pager.acquire_root();
    ScopeGuard guard = [&pager, &trunk, &root] {
        pager.release(std::move(root));
        pager.release(std::move(trunk));
    };

    auto free_head = FileHeader::get_freelist_head(root.constant_ptr());
    if (free_head.is_null()) {
        // Freelist is empty.
        return Status::not_found();
    } else if (free_head.value > pager.page_count()) {
        return Status::corruption();
    }
    auto s = pager.acquire(free_head, trunk);
    if (s.is_ok()) {
        const auto n = get_u32(trunk.constant_ptr() + sizeof(U32));
        if (n > kTrunkCapacity) {
            return Status::corruption();
        }
        if (n > 0) {
            pager.mark_dirty(trunk);
            auto *ptr = get_leaf_ptr(trunk.mutable_ptr(), n - 1);
            id_out.value = get_u32(ptr);
            put_u32(ptr, 0);
            put_u32(trunk.mutable_ptr() + sizeof(U32), n - 1);
        } else {
            id_out = free_head;
            free_head.value = get_u32(trunk.constant_ptr());
            pager.mark_dirty(root);
            FileHeader::put_freelist_head(root.mutable_ptr(), free_head);
            if (!free_head.is_null()) {
                s = PointerMap::write_entry(
                    pager, free_head, {Id::null(), PointerMap::kFreelistTrunk});
            }
        }
    }
    return s;
}

auto Freelist::assert_state(Pager &pager) -> bool
{
    Page head;
    auto root = pager.acquire_root();
    ScopeGuard guard = [&pager, &head, &root] {
        pager.release(std::move(root));
        pager.release(std::move(head));
    };

    auto free_head = FileHeader::get_freelist_head(root.constant_ptr());
    CALICODB_EXPECT_LE(free_head.value, pager.page_count());
    CALICODB_EXPECT_TRUE(free_head.is_null() || free_head.value > kFirstMapPage);

    Status s;
    Id last_id;
    while (!free_head.is_null()) {
        s = pager.acquire(free_head, head);
        CALICODB_EXPECT_TRUE(s.is_ok());
        const auto n = get_u32(head.constant_ptr() + sizeof(U32));
        CALICODB_EXPECT_LE(n, kTrunkCapacity);

        PointerMap::Entry entry;
        s = PointerMap::read_entry(pager, free_head, entry);
        CALICODB_EXPECT_TRUE(s.is_ok());
        CALICODB_EXPECT_EQ(entry.back_ptr, last_id);
        CALICODB_EXPECT_EQ(entry.type, PointerMap::kFreelistTrunk);

        for (std::size_t i = 0; i < n; ++i) {
            const Id leaf_id(get_u32(get_leaf_ptr(head.constant_ptr(), i)));
            CALICODB_EXPECT_FALSE(leaf_id.is_null());
            CALICODB_EXPECT_LE(leaf_id.value, pager.page_count());
            s = PointerMap::read_entry(pager, leaf_id, entry);
            CALICODB_EXPECT_TRUE(s.is_ok());
            // Leaf back pointers must point to the parent trunk page.
            CALICODB_EXPECT_EQ(entry.back_ptr, free_head);
            CALICODB_EXPECT_EQ(entry.type, PointerMap::kFreelistLeaf);
        }
        last_id = free_head;
        free_head.value = get_u32(head.constant_ptr());
        pager.release(std::move(head));
    }
    return true;
}

} // namespace calicodb
