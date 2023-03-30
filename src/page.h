// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_PAGE_H
#define CALICODB_PAGE_H

#include "encoding.h"
#include "frames.h"
#include "header.h"
#include "utils.h"

namespace calicodb
{

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

class Page
{
    Id m_id;
    std::size_t m_size = 0;
    CacheEntry *m_entry = nullptr;
    char *m_data = nullptr;
    bool m_write = false;

public:
    friend class FrameManager;
    friend struct Node;

    explicit Page() = default;

    [[nodiscard]] auto is_writable() const -> bool
    {
        return m_write;
    }

    [[nodiscard]] auto id() const -> Id
    {
        return m_id;
    }

    [[nodiscard]] auto entry() const -> const CacheEntry *
    {
        return m_entry;
    }

    [[nodiscard]] auto entry() -> CacheEntry *
    {
        return m_entry;
    }

    [[nodiscard]] auto view(std::size_t offset) const -> Slice
    {
        return Slice(m_data, m_size).advance(offset);
    }

    [[nodiscard]] auto view(std::size_t offset, std::size_t size) const -> Slice
    {
        return Slice(m_data, m_size).range(offset, size);
    }

    [[nodiscard]] auto data() -> char *
    {
        return m_data;
    }

    [[nodiscard]] auto data() const -> const char *
    {
        return m_data;
    }

    [[nodiscard]] auto size() const -> std::size_t
    {
        return m_size;
    }

    // Disable copies but allow moves.
    Page(const Page &) = delete;
    auto operator=(const Page &) -> Page & = delete;
    Page(Page &&) noexcept = default;
    auto operator=(Page &&) noexcept -> Page & = default;
};

[[nodiscard]] inline auto page_offset(const Page &page) -> std::size_t
{
    return FileHeader::kSize * page.id().is_root();
}

// TODO: These make more sense and can be used for frames as well.
[[nodiscard]] inline auto page_offset(Id page_id) -> std::size_t
{
    return FileHeader::kSize * page_id.is_root();
}

} // namespace calicodb

#endif // CALICODB_PAGE_H
