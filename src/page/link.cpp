#include "link.h"

#include "utils/encoding.h"
#include "utils/layout.h"

namespace calico {

Link::Link(Page page)
    : m_page {std::move(page)} {}

auto Link::next_id() const -> PID
{
    const auto offset = LinkLayout::header_offset() + LinkLayout::NEXT_ID_OFFSET;
    return PID {m_page.get_u32(offset)};
}

auto Link::set_next_id(PID id) -> void
{
    const auto offset = LinkLayout::header_offset() + LinkLayout::NEXT_ID_OFFSET;
    m_page.put_u32(offset, id.value);
}

auto Link::content_size() const -> Size
{
    return m_page.size() - LinkLayout::content_offset();
}

auto Link::ref_content() const -> BytesView
{
    const auto offset = LinkLayout::content_offset();
    return m_page.range(offset, m_page.size() - offset);
}

auto Link::mut_content(Size size) -> Bytes
{
    // Takes a size parameter to avoid updating more of the page than is necessary. See
    // Page::do_change() in page.cpp.
    return m_page.mut_range(LinkLayout::content_offset(), size);
}

} // calico