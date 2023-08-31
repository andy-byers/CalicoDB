// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "freelist.h"
#include "header.h"
#include "pager.h"

namespace calicodb
{

namespace
{

constexpr size_t kTrunkCapacity = (kPageSize - 2 * sizeof(uint32_t)) / sizeof(uint32_t);

struct FreePage {
    static auto get_leaf_ptr(PageRef &ref, size_t index) -> char *
    {
        return ref.data + (index + 2) * sizeof(uint32_t);
    }

    static auto get_leaf_ptr(const PageRef &ref, size_t index) -> const char *
    {
        return ref.data + (index + 2) * sizeof(uint32_t);
    }

    static auto get_next_id(PageRef &ref) -> Id
    {
        return Id(get_u32(ref.data));
    }

    static auto get_leaf_count(const PageRef &ref) -> uint32_t
    {
        return get_u32(ref.data + sizeof(uint32_t));
    }

    static auto get_leaf_id(const PageRef &ref, size_t index) -> Id
    {
        return Id(get_u32(FreePage::get_leaf_ptr(ref, index)));
    }

    static auto put_next_id(PageRef &ref, Id value) -> void
    {
        put_u32(ref.data, value.value);
    }

    static auto put_leaf_count(PageRef &ref, uint32_t value) -> void
    {
        put_u32(ref.data + sizeof(uint32_t), value);
    }

    static auto put_leaf_id(PageRef &ref, size_t index, uint32_t value) -> void
    {
        put_u32(get_leaf_ptr(ref, index), value);
    }
};

template <class T>
auto abs_distance(T t1, T t2) -> int64_t
{
    static_assert(std::is_signed_v<T> || sizeof(T) < sizeof(int64_t));
    return std::abs(static_cast<int64_t>(t1) - static_cast<int64_t>(t2));
}

} // namespace

auto Freelist::add(Pager &pager, PageRef *&page) -> Status
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
            const auto n = FreePage::get_leaf_count(*trunk);
            if (n < kTrunkCapacity) {
                // Trunk has enough room for a new leaf page.
                pager.mark_dirty(*trunk);
                FreePage::put_leaf_count(*trunk, n + 1);
                FreePage::put_leaf_id(*trunk, n, page->page_id.value);
                s = PointerMap::write_entry(
                    pager, page->page_id, {Id::null(), PointerMap::kFreelistPage});
                goto cleanup;
            } else if (n > kTrunkCapacity) {
                s = Status::corruption();
                goto cleanup;
            }
        }
    }
    if (s.is_ok()) {
        // `page` must become a new freelist trunk page. Update the file header to reflect this.
        pager.mark_dirty(root);
        FileHdr::put_freelist_head(root.data, page->page_id);
        // Transform `page` into a blank freelist trunk page that points at what was previously the first
        // trunk page.
        pager.mark_dirty(*page);
        FreePage::put_next_id(*page, free_head);
        FreePage::put_leaf_count(*page, 0);
        // Point the new head's back pointer at Id::null().
        free_head = page->page_id;
        // Release the page before it gets discarded below.
        pager.release(page);
        s = PointerMap::write_entry(
            pager, free_head,
            {Id::null(), PointerMap::kFreelistPage});
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

// Translated from SQLite. This is like allocateBtreePage(), except allocating from the end of
// the file is performed in Pager::allocate() and only 2 behaviors are supported: kPopExact and
// kPopAny.
auto Freelist::remove(Pager &pager, RemoveType type, Id nearby, PageRef *&page_out) -> Status
{
    page_out = nullptr;

    CALICODB_EXPECT_TRUE(type == kRemoveAny || !nearby.is_null());
    auto database_root = &pager.get_root();
    const auto max_page = pager.page_count();
    const auto free_count = FileHdr::get_freelist_length(database_root->data);
    if (free_count >= max_page) {
        return Status::corruption();
    } else if (free_count == 0) {
        return Status::ok();
    }
    Id trunk_id;
    auto search_list = false;
    uint32_t search_attempts = 0;

    if (type == kRemoveExact && nearby.value <= max_page) {
        PointerMap::Entry entry;
        assert(!nearby.is_null());
        auto s = PointerMap::read_entry(pager, nearby, entry);
        if (!s.is_ok()) {
            return s;
        }
        if (entry.type == PointerMap::kFreelistPage) {
            search_list = true;
        }
    }
    pager.mark_dirty(*database_root);
    FileHdr::put_freelist_length(database_root->data, free_count - 1);

    Status s;
    PageRef *trunk = nullptr;
    PageRef *prev_trunk = nullptr;
    uint32_t leaf_count;
    do {
        prev_trunk = trunk;
        if (prev_trunk) {
            trunk_id = FreePage::get_next_id(*prev_trunk);
        } else {
            trunk_id = FileHdr::get_freelist_head(database_root->data);
        }
        if (trunk_id.value > max_page || search_attempts++ > free_count) {
            s = Status::corruption();
        } else {
            s = pager.acquire(trunk_id, trunk);
        }
        if (!s.is_ok()) {
            trunk = nullptr;
            goto cleanup;
        }
        assert(trunk != nullptr);
        assert(trunk->data != nullptr);
        leaf_count = FreePage::get_leaf_count(*trunk);
        if (leaf_count == 0 && !search_list) {
            CALICODB_EXPECT_EQ(prev_trunk, nullptr);
            pager.mark_dirty(*trunk);
            std::memcpy(database_root->data + FileHdr::kFreelistHeadOffset,
                        trunk->data, sizeof(uint32_t));
            page_out = trunk;
            trunk = nullptr;
        } else if (leaf_count > kTrunkCapacity) {
            s = Status::corruption();
            goto cleanup;
        } else if (search_list && nearby == trunk_id) {
            page_out = trunk;
            search_list = false;
            pager.mark_dirty(*trunk);
            if (leaf_count == 0) {
                if (prev_trunk) {
                    pager.mark_dirty(*prev_trunk);
                    std::memcpy(prev_trunk->data, trunk->data, sizeof(uint32_t));
                } else {
                    std::memcpy(database_root->data + FileHdr::kFreelistHeadOffset,
                                trunk->data, sizeof(uint32_t));
                }
            } else {
                PageRef *new_trunk;
                const auto new_id = FreePage::get_leaf_id(*trunk, 0);
                if (new_id.value > max_page) {
                    s = Status::corruption();
                    goto cleanup;
                }
                s = pager.acquire(new_id, new_trunk);
                if (!s.is_ok()) {
                    goto cleanup;
                }
                pager.mark_dirty(*new_trunk);
                std::memcpy(new_trunk->data, trunk->data, sizeof(uint32_t));
                FreePage::put_leaf_count(*new_trunk, leaf_count - 1);
                std::memcpy(FreePage::get_leaf_ptr(*new_trunk, 0),
                            FreePage::get_leaf_ptr(*trunk, 1),
                            (leaf_count - 1) * sizeof(uint32_t));
                pager.release(new_trunk);
                if (prev_trunk) {
                    pager.mark_dirty(*prev_trunk);
                    put_u32(prev_trunk->data, new_id.value);
                } else {
                    FileHdr::put_freelist_head(database_root->data, new_id);
                }
            }
            trunk = nullptr;
        } else if (leaf_count > 0) {
            Id page_id;
            uint32_t closest = 0;
            if (!nearby.is_null()) {
                auto smallest = abs_distance(
                    FreePage::get_leaf_id(*trunk, 0).value,
                    nearby.value);
                for (uint32_t i = 1; i < leaf_count; i++) {
                    const auto dist = abs_distance(
                        FreePage::get_leaf_id(*trunk, i).value,
                        nearby.value);
                    if (dist < smallest) {
                        closest = i;
                        smallest = dist;
                    }
                }
            }

            page_id = FreePage::get_leaf_id(*trunk, closest);
            if (page_id.value > max_page || page_id.value < 2) {
                s = Status::corruption();
                goto cleanup;
            }
            if (!search_list || page_id == nearby) {
                pager.mark_dirty(*trunk);
                if (closest < leaf_count - 1) {
                    std::memcpy(FreePage::get_leaf_ptr(*trunk, closest),
                                FreePage::get_leaf_ptr(*trunk, leaf_count - 1),
                                sizeof(uint32_t));
                }
                FreePage::put_leaf_count(*trunk, leaf_count - 1);
                s = pager.acquire(page_id, page_out);
                if (s.is_ok()) {
                    pager.mark_dirty(*page_out);
                }
                search_list = false;
            }
        }
        pager.release(prev_trunk);
        prev_trunk = nullptr;
    } while (search_list);

cleanup:
    pager.release(trunk);
    pager.release(prev_trunk);
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
    while (!free_head.is_null()) {
        s = pager.acquire(free_head, head);
        CALICODB_EXPECT_TRUE(s.is_ok());
        const auto n = FreePage::get_leaf_count(*head);
        CALICODB_EXPECT_LE(n, kTrunkCapacity);

        PointerMap::Entry entry;
        s = PointerMap::read_entry(pager, free_head, entry);
        CALICODB_EXPECT_TRUE(s.is_ok());
        CALICODB_EXPECT_EQ(entry.back_ptr, Id::null());
        CALICODB_EXPECT_EQ(entry.type, PointerMap::kFreelistPage);

        for (size_t i = 0; i < n; ++i) {
            const auto leaf_id = FreePage::get_leaf_id(*head, i);
            CALICODB_EXPECT_FALSE(leaf_id.is_null());
            CALICODB_EXPECT_LE(leaf_id.value, pager.page_count());
            s = PointerMap::read_entry(pager, leaf_id, entry);
            CALICODB_EXPECT_TRUE(s.is_ok());
            CALICODB_EXPECT_EQ(entry.back_ptr, Id::null());
            CALICODB_EXPECT_EQ(entry.type, PointerMap::kFreelistPage);
        }
        free_head = FreePage::get_next_id(*head);
        pager.release(head);
    }
    return true;
}

} // namespace calicodb
