// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_PAGE_H
#define CALICODB_PAGE_H

#include "delta.h"
#include "encoding.h"
#include "header.h"

namespace calicodb
{

using PageSize = std::uint16_t;

struct LogicalPageId {
    [[nodiscard]] static auto with_page(Id pid) -> LogicalPageId
    {
        return LogicalPageId {Id::null(), pid};
    }

    [[nodiscard]] static auto with_table(Id tid) -> LogicalPageId
    {
        return LogicalPageId {tid, Id::null()};
    }

    [[nodiscard]] static auto root() -> LogicalPageId
    {
        return LogicalPageId {Id::root(), Id::root()};
    }

    // Results in "LogicalPageId(Id::null, Id::null())".
    explicit LogicalPageId() = default;

    explicit LogicalPageId(Id tid, Id pid)
        : table_id {tid},
          page_id {pid}
    {
    }

    static constexpr std::size_t kSize {16};

    Id table_id;
    Id page_id;
};

class Page
{
    std::vector<PageDelta> m_deltas;
    Id m_id;
    std::size_t m_size {};
    char *m_data {};
    bool m_write {};

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

    [[nodiscard]] auto view(std::size_t offset) const -> Slice
    {
        return Slice {m_data, m_size}.advance(offset);
    }

    [[nodiscard]] auto view(std::size_t offset, std::size_t size) const -> Slice
    {
        return Slice {m_data, m_size}.range(offset, size);
    }

    auto mutate(std::size_t offset, std::size_t size) -> char *
    {
        CALICODB_EXPECT_TRUE(m_write);
        insert_delta(m_deltas, PageDelta {offset, size});
        return m_data + offset;
    }

    [[nodiscard]] auto data() const -> const char *
    {
        return m_data;
    }

    [[nodiscard]] auto data() -> char *
    {
        return m_data;
    }

    [[nodiscard]] auto size() const -> std::size_t
    {
        return m_size;
    }

    [[nodiscard]] auto has_changes() const -> bool
    {
        return !m_deltas.empty();
    }

    [[nodiscard]] auto deltas() -> const std::vector<PageDelta> &
    {
        compress_deltas(m_deltas);
        return m_deltas;
    }

    auto TEST_populate(Id page_id, char *data, std::size_t size, bool write, const std::vector<PageDelta> &deltas = {}) -> void
    {
        m_id = page_id;
        m_data = data;
        m_size = size;
        m_write = write;
        m_deltas = deltas;
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

[[nodiscard]] inline auto read_page_lsn(const Page &page) -> Lsn
{
    return Lsn {get_u64(page.data() + page_offset(page))};
}

inline auto write_page_lsn(Page &page, Lsn lsn) -> void
{
    put_u64(page.mutate(page_offset(page), sizeof(Lsn)), lsn.value);
}

// TODO: These make more sense and can be used for frames as well.
[[nodiscard]] inline auto page_offset(Id page_id) -> std::size_t
{
    return FileHeader::kSize * page_id.is_root();
}

[[nodiscard]] inline auto read_page_lsn(Id page_id, const char *data) -> Lsn
{
    return Lsn {get_u64(data + page_offset(page_id))};
}

inline auto write_page_lsn(Id page_id, Lsn lsn, char *data) -> void
{
    put_u64(data + page_offset(page_id), lsn.value);
}

} // namespace calicodb

#endif // CALICODB_PAGE_H
