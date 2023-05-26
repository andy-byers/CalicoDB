// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_PAGE_H
#define CALICODB_PAGE_H

#include "utils.h"

namespace calicodb
{

class Pager;
struct PageRef;

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
    explicit Page(Pager &pager, PageRef &ref);
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
        return Slice(m_data, kPageSize);
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

[[nodiscard]] auto page_offset(Id page_id) -> std::size_t;
[[nodiscard]] auto read_next_id(const Page &page) -> Id;
auto write_next_id(Page &page, Id next_id) -> void;

} // namespace calicodb

#endif // CALICODB_PAGE_H
