// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_PAGE_H
#define CALICODB_PAGE_H

#include "encoding.h"
#include "header.h"
#include "utils.h"

namespace calicodb
{

class Pager;

struct PageRef final {
    Id page_id;

    // Pointer to the start of the buffer slot containing the page data.
    char *page = nullptr;

    // Number of live copies of this page.
    unsigned refcount = 0;

    // Dirty list fields.
    PageRef *prev = nullptr;
    PageRef *next = nullptr;

    enum Flag {
        kNormal = 0,
        kDirty = 1,
        kExtra = 2,
    } flag = kNormal;
};

class Page final
{
    Pager *m_pager = nullptr;
    PageRef *m_ref = nullptr;
    char *m_data = nullptr;
    Id m_id;
    bool m_write = false;

public:
    friend class Pager;
    friend struct Node;

    explicit Page() = default;
    explicit Page(Pager &pager, PageRef &ref)
        : m_pager(&pager),
          m_ref(&ref),
          m_data(ref.page),
          m_id(ref.page_id)
    {
    }

    ~Page();

    [[nodiscard]] auto is_writable() const -> bool
    {
        return m_write;
    }

    [[nodiscard]] auto id() const -> Id
    {
        return m_id;
    }

    [[nodiscard]] auto view() const -> Slice
    {
        return {m_data, kPageSize};
    }

    [[nodiscard]] auto data() -> char *
    {
        return m_data;
    }

    [[nodiscard]] auto data() const -> const char *
    {
        return m_data;
    }

    // Disable copies.
    Page(const Page &) = delete;
    auto operator=(const Page &) -> Page & = delete;

    // Custom move to handle NULLing out the Pager pointer.
    Page(Page &&rhs) noexcept;
    auto operator=(Page &&rhs) noexcept -> Page &;
};

[[nodiscard]] inline auto page_offset(Id page_id) -> std::size_t
{
    return FileHeader::kSize * page_id.is_root();
}

[[nodiscard]] inline auto read_next_id(const Page &page) -> Id
{
    return Id(get_u32(page.data() + page_offset(page.id())));
}

inline auto write_next_id(Page &page, Id next_id) -> void
{
    put_u32(page.data() + page_offset(page.id()), next_id.value);
}

} // namespace calicodb

#endif // CALICODB_PAGE_H
