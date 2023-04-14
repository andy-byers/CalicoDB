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

struct LogicalPageId {
    [[nodiscard]] static auto with_page(Id pid) -> LogicalPageId
    {
        return LogicalPageId(Id::null(), pid);
    }

    [[nodiscard]] static auto with_table(Id tid) -> LogicalPageId
    {
        return LogicalPageId(tid, Id::null());
    }

    [[nodiscard]] static auto root() -> LogicalPageId
    {
        return LogicalPageId(Id::root(), Id::root());
    }

    // Results in "LogicalPageId(Id::null, Id::null())".
    explicit LogicalPageId() = default;

    explicit LogicalPageId(Id tid, Id pid)
        : table_id(tid),
          page_id(pid)
    {
    }

    static constexpr std::size_t kSize = Id::kSize * 2;

    Id table_id;
    Id page_id;
};

class Page final
{
    Pager *m_pager = nullptr;
    PageRef *m_ref = nullptr;
    char *m_data = nullptr;
    std::size_t m_size = 0;
    Id m_id;
    bool m_write = false;

public:
    friend class BufferManager;
    friend class Pager;
    friend struct Node;

    explicit Page() = default;
    explicit Page(Pager &pager, PageRef &ref);
    ~Page();

    [[nodiscard]] auto is_writable() const -> bool;
    [[nodiscard]] auto id() const -> Id;
    [[nodiscard]] auto view() const -> Slice;
    [[nodiscard]] auto data() -> char *;
    [[nodiscard]] auto data() const -> const char *;
    [[nodiscard]] auto size() const -> std::size_t;

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
