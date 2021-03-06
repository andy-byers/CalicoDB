#include "link.h"

#include "utils/encoding.h"
#include "utils/layout.h"

namespace cco {

Link::Link(Page page)
    : m_page {std::move(page)} {}

auto Link::next_id() const -> PID
{
    const auto offset = LinkLayout::header_offset() + LinkLayout::NEXT_ID_OFFSET;
    return PID {get_u32(m_page, offset)};
}

auto Link::set_next_id(PID id) -> void
{
    const auto offset = LinkLayout::header_offset() + LinkLayout::NEXT_ID_OFFSET;
    put_u32(m_page, offset, id.value);
}

auto Link::content_size() const -> Size
{
    return m_page.size() - LinkLayout::content_offset();
}

auto Link::content_view() const -> BytesView
{
    const auto offset = LinkLayout::content_offset();
    return m_page.view(offset, m_page.size() - offset);
}

auto Link::content_bytes(Size size) -> Bytes
{
    // Takes a size parameter to avoid updating more of the page than is necessary. See
    // Page::do_change() in page.cpp.
    return m_page.bytes(LinkLayout::content_offset(), size);
}

} // cco