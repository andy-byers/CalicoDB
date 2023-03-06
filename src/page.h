#ifndef CALICODB_PAGE_H
#define CALICODB_PAGE_H

#include "delta.h"
#include "encoding.h"
#include "header.h"
#include "types.h"

namespace calicodb
{

using PageSize = std::uint16_t;

struct LogicalPageId {
    [[nodiscard]] static auto unknown_table(Id pid) -> LogicalPageId
    {
        return LogicalPageId {Id::null(), pid};
    }

    [[nodiscard]] static auto unknown_page(Id tid) -> LogicalPageId
    {
        return LogicalPageId {tid, Id::null()};
    }

    [[nodiscard]] static auto unknown() -> LogicalPageId
    {
        return LogicalPageId {Id::null(), Id::null()};
    }

    [[nodiscard]] static auto root() -> LogicalPageId
    {
        return LogicalPageId {Id::root(), Id::root()};
    }

    explicit LogicalPageId(Id tid, Id pid)
        : table_id {tid},
          page_id {pid}
    {
    }

    Id table_id;
    Id page_id;
};

class Page
{
    ChangeBuffer m_deltas;
    LogicalPageId m_id;
    Span m_span;
    Id m_table_id;
    Id m_page_id;
    bool m_write {};

public:
    friend class Frame;
    friend class Pager;
    friend struct Node;

    explicit Page(LogicalPageId id)
        : m_id {id}
    {
    }

    [[nodiscard]] auto is_writable() const -> bool
    {
        return m_write;
    }

    [[nodiscard]] auto id() const -> LogicalPageId
    {
        return m_id;
    }

    [[nodiscard]] auto view(std::size_t offset) const -> Slice
    {
        return m_span.range(offset);
    }

    [[nodiscard]] auto view(std::size_t offset, std::size_t size) const -> Slice
    {
        return m_span.range(offset, size);
    }

    [[nodiscard]] auto span(std::size_t offset, std::size_t size) -> Span
    {
        CDB_EXPECT_TRUE(m_write);
        insert_delta(m_deltas, PageDelta {offset, size});
        return m_span.range(offset, size);
    }

    [[nodiscard]] auto data() const -> const char *
    {
        return m_span.data();
    }

    [[nodiscard]] auto data() -> char *
    {
        return m_span.data();
    }

    [[nodiscard]] auto size() const -> std::size_t
    {
        return m_span.size();
    }

    [[nodiscard]] auto deltas() -> const ChangeBuffer &
    {
        CDB_EXPECT_TRUE(m_write);
        compress_deltas(m_deltas);
        return m_deltas;
    }

    auto TEST_populate(Span buffer, bool write, const ChangeBuffer &deltas = {}) -> void
    {
        m_span = buffer;
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
    return FileHeader::kSize * page.id().page_id.is_root();
}

[[nodiscard]] inline auto read_page_lsn(const Page &page) -> Lsn
{
    return Lsn {get_u64(page.data() + page_offset(page))};
}

inline auto write_page_lsn(Page &page, Lsn lsn) -> void
{
    put_u64(page.span(page_offset(page), sizeof(Lsn)), lsn.value);
}

} // namespace calicodb

#endif // CALICODB_PAGE_H
