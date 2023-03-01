#ifndef CALICODB_PAGE_H
#define CALICODB_PAGE_H

#include "delta.h"
#include "encoding.h"
#include "header.h"
#include "types.h"

namespace calicodb
{

using PageSize = std::uint16_t;

class Page
{
    ChangeBuffer m_deltas;
    Span m_span;
    Id m_id;
    bool m_write {};

public:
    friend struct FileHeader;
    friend struct NodeHeader;
    friend struct Node;
    friend class Frame;

    Page() = default;

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

    auto TEST_populate(Id id, Span buffer, bool write, const ChangeBuffer &deltas = {}) -> void
    {
        m_id = id;
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
    return FileHeader::SIZE * page.id().is_root();
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
