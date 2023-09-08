// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_PAGE_H
#define CALICODB_PAGE_H

#include "header.h"
#include "list.h"
#include "mem.h"

namespace calicodb
{

struct PageRef;

// Allocate this many bytes of extra space at the end of each page buffer to catch
// out-of-bounds reads and writes that might occur if the database is corrupted.
static constexpr size_t kSpilloverLen = sizeof(void *);

struct DirtyHdr {
    DirtyHdr *dirty;
    DirtyHdr *prev_entry;
    DirtyHdr *next_entry;

    [[nodiscard]] inline auto get_page_ref() -> PageRef *;
    [[nodiscard]] inline auto get_page_ref() const -> const PageRef *;
};

static_assert(std::is_trivially_copyable_v<DirtyHdr>);

struct PageRef {
    char *data;
    PageRef *next_hash;
    PageRef *next_extra;
    PageRef *prev_entry;
    PageRef *next_entry;
    DirtyHdr dirty_hdr;

    Id page_id;
    uint16_t refs;

    enum Flag : uint16_t {
        kNormal = 0,
        kCached = 1,
        kDirty = 2,
        kAppend = 4,
    } flag;

    [[nodiscard]] auto key() const -> uint32_t
    {
        return page_id.value;
    }

    static auto alloc(size_t page_size) -> PageRef *
    {
        auto *ref = static_cast<PageRef *>(Mem::allocate(
            sizeof(PageRef) + page_size + kSpilloverLen));
        if (ref) {
            CALICODB_EXPECT_TRUE(is_aligned(ref, alignof(PageRef)));
            init(*ref, reinterpret_cast<char *>(ref + 1));
        }
        return ref;
    }

    static auto init(PageRef &ref, char *page) -> void
    {
        IntrusiveList::initialize(ref);
        IntrusiveList::initialize(ref.dirty_hdr);
        ref = {
            page,
            nullptr,
            nullptr,
            // Next 3 members already set by IntrusiveList::initialize(). Forward the values.
            ref.prev_entry,
            ref.next_entry,
            ref.dirty_hdr,
            Id::null(),
            0,
            PageRef::kNormal,
        };
    }

    static auto free(PageRef *ref) -> void
    {
        Mem::deallocate(ref);
    }

    [[nodiscard]] auto get_flag(Flag f) const -> bool
    {
        return flag & f;
    }
    auto set_flag(Flag f) -> void
    {
        flag = static_cast<Flag>(flag | f);
    }
    auto clear_flag(Flag f) -> void
    {
        flag = static_cast<Flag>(flag & ~f);
    }
};

static_assert(std::is_trivially_copyable_v<PageRef>);

auto DirtyHdr::get_page_ref() -> PageRef *
{
    auto *bytes = reinterpret_cast<char *>(this);
    return reinterpret_cast<PageRef *>(bytes - offsetof(PageRef, dirty_hdr));
}

auto DirtyHdr::get_page_ref() const -> const PageRef *
{
    const auto *bytes = reinterpret_cast<const char *>(this);
    return reinterpret_cast<const PageRef *>(bytes - offsetof(PageRef, dirty_hdr));
}

[[nodiscard]] inline auto page_offset(Id page_id) -> uint32_t
{
    return FileHdr::kSize * page_id.is_root();
}

} // namespace calicodb

#endif // CALICODB_PAGE_H
