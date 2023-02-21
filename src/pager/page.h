#ifndef CALICO_PAGER_PAGE_H
#define CALICO_PAGER_PAGE_H

#include "pager/delta.h"
#include "tree/header.h"
#include "utils/encoding.h"
#include "utils/types.h"

namespace Calico {

using PageSize = std::uint16_t;

class Page {
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

    [[nodiscard]]
    auto is_writable() const -> bool
    {
        return m_write;
    }

    [[nodiscard]]
    auto id() const -> Id
    {
        return m_id;
    }

    [[nodiscard]]
    auto view(Size offset) const -> Slice
    {
        return m_span.range(offset);
    }

    [[nodiscard]]
    auto view(Size offset, Size size) const -> Slice
    {
        return m_span.range(offset, size);
    }

    [[nodiscard]]
    auto span(Size offset, Size size) -> Span
    {
        CALICO_EXPECT_TRUE(m_write);
        insert_delta(m_deltas, PageDelta {offset, size});
        return m_span.range(offset, size);
    }

    [[nodiscard]]
    auto data() const -> const Byte *
    {
        return m_span.data();
    }

    [[nodiscard]]
    auto data() -> Byte *
    {
        return m_span.data();
    }

    [[nodiscard]]
    auto size() const -> Size
    {
        return m_span.size();
    }

    [[nodiscard]]
    auto deltas() -> const ChangeBuffer &
    {
        CALICO_EXPECT_TRUE(m_write);
        compress_deltas(m_deltas);
        return m_deltas;
    }

    // Disable copies but allow moves.
    Page(const Page &) = delete;
    auto operator=(const Page &) -> Page & = delete;
    Page(Page &&) noexcept = default;
    auto operator=(Page &&) noexcept -> Page & = default;
};

[[nodiscard]]
inline auto page_offset(const Page &page) -> Size
{
    return FileHeader::SIZE * page.id().is_root();
}

[[nodiscard]]
inline auto read_page_lsn(const Page &page) -> Lsn
{
    return Lsn {get_u64(page.data() + page_offset(page))};
}

inline auto write_page_lsn(Page &page, Lsn lsn) -> void
{
    put_u64(page.span(page_offset(page), sizeof(Lsn)), lsn.value);
}

} // namespace Calico

#endif // CALICO_PAGER_PAGE_H
