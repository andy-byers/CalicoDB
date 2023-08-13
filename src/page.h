// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_PAGE_H
#define CALICODB_PAGE_H

#include "alloc.h"
#include "header.h"
#include "list.h"

namespace calicodb
{

struct PageRef;

struct DirtyHdr {
    DirtyHdr *dirty;
    DirtyHdr *prev_entry;
    DirtyHdr *next_entry;

    [[nodiscard]] inline auto get_page_ref() -> PageRef *;
    [[nodiscard]] inline auto get_page_ref() const -> const PageRef *;
};

struct PageRef {
    char *data;
    PageRef *prev_entry;
    PageRef *next_entry;

    Id page_id;
    uint16_t refs;

    enum Flag : uint16_t {
        kNormal = 0,
        kCached = 1,
        kDirty = 2,
        kExtra = 4,
    } flag;

    static auto alloc() -> PageRef *
    {
        static_assert(std::is_trivially_copyable_v<DirtyHdr>);
        static_assert(std::is_trivially_copyable_v<PageRef>);
        static_assert(alignof(PageRef) == alignof(void *));
        static_assert(alignof(DirtyHdr) == alignof(void *));
        static_assert(sizeof(PageRef) == 4 * sizeof(void *));

        // Allocate this many bytes of extra space at the end of the page buffer to catch
        // out-of-bounds reads and writes that might occur if the database is corrupted.
        static constexpr size_t kSpilloverLen = alignof(PageRef);

        auto *ref = static_cast<PageRef *>(Alloc::alloc(
            sizeof(PageRef) +
                sizeof(DirtyHdr) +
                kPageSize +
                kSpilloverLen,
            alignof(PageRef)));
        if (ref) {
            *ref = {
                reinterpret_cast<char *>(ref + 1) + sizeof(DirtyHdr),
                ref,
                ref,
                Id::null(),
                0,
                PageRef::kNormal,
            };

            auto *hdr = ref->get_dirty_hdr();
            *hdr = {
                nullptr,
                hdr,
                hdr,
            };

            // DirtyHdr must be properly aligned.
            CALICODB_EXPECT_EQ(0, reinterpret_cast<std::uintptr_t>(hdr) % alignof(DirtyHdr));
        }
        return ref;
    }

    static auto free(PageRef *ref) -> void
    {
        Alloc::free(ref);
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

    [[nodiscard]] auto get_dirty_hdr() -> DirtyHdr *
    {
        return reinterpret_cast<DirtyHdr *>(this + 1);
    }
    [[nodiscard]] auto get_dirty_hdr() const -> const DirtyHdr *
    {
        return const_cast<PageRef *>(this)->get_dirty_hdr();
    }
};

auto DirtyHdr::get_page_ref() -> PageRef *
{
    return reinterpret_cast<PageRef *>(this) - 1;
}

auto DirtyHdr::get_page_ref() const -> const PageRef *
{
    return const_cast<DirtyHdr *>(this)->get_page_ref();
}

[[nodiscard]] inline auto page_offset(Id page_id) -> uint32_t
{
    return FileHdr::kSize * page_id.is_root();
}

} // namespace calicodb

#endif // CALICODB_PAGE_H
