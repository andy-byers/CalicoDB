#ifndef CALICO_TREE_PAGE_H
#define CALICO_TREE_PAGE_H

#include "header.h"
#include "page/delta.h"
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

    explicit Page(Id id, Span span, bool write)
        : m_span {span},
          m_id {id},
          m_write {write}
    {}

    ~Page()
    {
        // TODO: For debugging.
//        CALICO_EXPECT_TRUE(m_deltas.empty());
    }

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
    auto take() -> ChangeBuffer
    {
        CALICO_EXPECT_TRUE(m_write);
        compress_deltas(m_deltas);
        return std::exchange(m_deltas, {});
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

} // namespace Calico

#endif // CALICO_TREE_PAGE_H
