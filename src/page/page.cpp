#include "page.h"
#include "update.h"

#include "pool/interface.h"
#include "utils/encoding.h"
#include "utils/layout.h"

namespace cco::page {

using namespace utils;

Page::Page(const Parameters &param):
      m_source {param.source},
      m_data {param.data},
      m_id {param.id},
      m_is_writable {param.is_writable},
      m_is_dirty {param.is_dirty}
{
    CCO_EXPECT_TRUE(is_power_of_two(m_data.size()));
    CCO_EXPECT_GE(m_data.size(), MINIMUM_PAGE_SIZE);
    CCO_EXPECT_LE(m_data.size(), MAXIMUM_PAGE_SIZE);
}

Page::~Page()
{
    if (m_source.is_valid())
        m_source->on_release(*this);
}

auto Page::header_offset() const -> Index
{
    return PageLayout::header_offset(m_id);
}

auto Page::type() const -> PageType
{
    return PageType {get_u16(*this, header_offset() + PageLayout::TYPE_OFFSET)};
}

auto Page::lsn() const -> LSN
{
    return LSN {get_u32(*this, header_offset() + PageLayout::LSN_OFFSET)};
}

auto Page::set_type(PageType type) -> void
{
    const auto offset = header_offset() + PageLayout::TYPE_OFFSET;
    put_u16(*this, offset, static_cast<uint16_t>(type));
}

auto Page::set_lsn(LSN lsn) -> void
{
    const auto offset = header_offset() + PageLayout::LSN_OFFSET;
    put_u32(*this, offset, lsn.value);
}

auto Page::id() const -> PID
{
    return m_id;
}

auto Page::size() const -> Size
{
    return m_data.size();
}

auto Page::view(Index offset) const -> BytesView
{
    return m_data.range(offset);
}

auto Page::view(Index offset, Size size) const -> BytesView
{
    return m_data.range(offset, size);
}

auto Page::read(Bytes out, Index offset) const -> void
{
    mem_copy(out, m_data.range(offset, out.size()));
}

auto Page::bytes(Index offset) -> Bytes
{
    return bytes(offset, size() - offset);
}

auto Page::bytes(Index offset, Size size) -> Bytes
{
    CCO_EXPECT_TRUE(m_is_writable);
    if (m_manager)
        m_manager->push({offset, size});
    m_is_dirty = true;
    return m_data.range(offset, size);
}

auto Page::write(BytesView in, Index offset) -> void
{
    mem_copy(bytes(offset, in.size()), in);
}

auto get_u16(const Page &page, Index offset) -> uint16_t
{
    return utils::get_u16(page.view(offset, sizeof(uint16_t)));
}

auto get_u32(const Page &page, Index offset) -> uint32_t
{
    return utils::get_u32(page.view(offset, sizeof(uint32_t)));
}

auto put_u16(Page &page, Index offset, uint16_t value) -> void
{
    utils::put_u16(page.bytes(offset, sizeof(value)), value);
}

auto put_u32(Page &page, Index offset, uint32_t value) -> void
{
    utils::put_u32(page.bytes(offset, sizeof(value)), value);
}

} // calico::page
