// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_PAGE_H
#define CALICODB_PAGE_H

#include "header.h"

namespace calicodb
{

struct PageRef;

struct DirtyHdr {
    DirtyHdr *dirty;
    DirtyHdr *prev;
    DirtyHdr *next;

    [[nodiscard]] inline auto get_page_ref() -> PageRef *;
    [[nodiscard]] inline auto get_page_ref() const -> const PageRef *;
};

struct PageRef {
    Id page_id;
    U32 refs;

    PageRef *prev;
    PageRef *next;

    enum Flag {
        kNormal = 0,
        kCached = 1,
        kDirty = 2,
        kExtra = 4,
    } flag;

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

    [[nodiscard]] auto get_data() -> char *
    {
        return reinterpret_cast<char *>(this + 1) + sizeof(DirtyHdr);
    }
    [[nodiscard]] auto get_data() const -> const char *
    {
        return const_cast<PageRef *>(this)->get_data();
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

inline auto alloc_page() -> PageRef *
{
    static_assert(std::is_trivially_copyable_v<DirtyHdr>);
    static_assert(std::is_trivially_copyable_v<PageRef>);
    static_assert(alignof(PageRef) == alignof(DirtyHdr));
    static constexpr std::size_t kSpilloverLen = alignof(PageRef);

    static constexpr std::size_t kPageRefSize =
        sizeof(PageRef) +
        sizeof(DirtyHdr) +
        kPageSize +
        kSpilloverLen;
    auto *ref = static_cast<PageRef *>(std::aligned_alloc(
        alignof(PageRef),
        kPageRefSize));
    std::memset(ref, 0, kPageRefSize);

    ref->page_id = Id::null();
    ref->flag = PageRef::kNormal;
    ref->refs = 0;
    ref->prev = ref;
    ref->next = ref;

    auto *hdr = ref->get_dirty_hdr();
    hdr->dirty = nullptr;
    hdr->prev = nullptr;
    hdr->next = nullptr;

    // DirtyHdr must be properly aligned.
    CALICODB_EXPECT_EQ(0, reinterpret_cast<std::uintptr_t>(hdr) % alignof(DirtyHdr));
    return ref;
}

inline auto free_page(PageRef *ref) -> void
{
    std::free(ref);
}

[[nodiscard]] inline auto page_offset(Id page_id) -> U32
{
    return FileHdr::kSize * page_id.is_root();
}

} // namespace calicodb

#endif // CALICODB_PAGE_H
