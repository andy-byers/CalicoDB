#include "link.h"

namespace cub {

Link::Link(Page page)
    : m_page{std::move(page)} {}

auto Link::next_id() const -> PID
{
    const auto offset = LinkLayout::header_offset() + LinkLayout::NEXT_ID_OFFSET;
    return PID{get_uint32(m_page.range(offset))};
}

auto Link::set_next_id(PID id) -> void
{
    const auto offset = LinkLayout::header_offset() + LinkLayout::NEXT_ID_OFFSET;
    put_uint32(m_page.mut_range(offset), id.value);
}

auto Link::ref_content() const -> RefBytes
{
    return m_page.range(LinkLayout::content_offset());
}

auto Link::mut_content() -> MutBytes
{
    return m_page.mut_range(LinkLayout::content_offset());
}

} // Cub