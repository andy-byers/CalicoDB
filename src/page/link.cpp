#include "link.h"

#include "utils/encoding.h"
#include "utils/layout.h"

namespace Calico {

Link::Link(Page page)
    : m_page {std::move(page)}
{}

auto Link::next_id() const -> Id
{
    const auto offset = LinkLayout::header_offset() + LinkLayout::NEXT_ID_OFFSET;
    return Id {get_u64(m_page, offset)};
}

auto Link::set_next_id(Id id) -> void
{
    const auto offset = LinkLayout::header_offset() + LinkLayout::NEXT_ID_OFFSET;
    put_u64(m_page, offset, id.value);
}

auto Link::content_size() const -> Size
{
    return m_page.size() - LinkLayout::content_offset();
}

auto Link::content_view() const -> Slice
{
    const auto offset = LinkLayout::content_offset();
    return m_page.view(offset, m_page.size() - offset);
}

auto Link::content_bytes(Size size) -> Span
{
    // Takes a size parameter to avoid updating more of the page than is necessary.
    return m_page.span(LinkLayout::content_offset(), size);
}

} // namespace Calico