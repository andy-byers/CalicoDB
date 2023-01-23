#include "page.h"
#include "delta.h"
#include "pager/pager.h"
#include "utils/encoding.h"
#include "utils/layout.h"
#include "wal/helpers.h"

namespace Calico {

Page::Page(const Parameters &param)
    : m_source {param.source},
      m_data {param.data},
      m_id {param.id},
      m_header_offset {PageLayout::header_offset(m_id)},
      m_is_writable {param.is_writable}
{
    CALICO_EXPECT_TRUE(is_power_of_two(m_data.size()));
    CALICO_EXPECT_GE(m_data.size(), MINIMUM_PAGE_SIZE);
    CALICO_EXPECT_LE(m_data.size(), MAXIMUM_PAGE_SIZE);
}

Page::~Page()
{
    if (m_source.is_valid())
        (void)std::move(m_source)->release(std::move(*this));
}

auto Page::type() const -> PageType
{
    return PageType {get_u16(*this,  m_header_offset + PageLayout::TYPE_OFFSET)};
}

auto Page::lsn() const -> Lsn
{
    return Lsn {get_u64(*this,  m_header_offset + PageLayout::LSN_OFFSET)};
}

auto Page::set_type(PageType type) -> void
{
    const auto offset =  m_header_offset + PageLayout::TYPE_OFFSET;
    put_u16(*this, offset, static_cast<std::uint16_t>(type));
}

auto Page::set_lsn(Id value) -> void
{
    CALICO_EXPECT_LE(lsn(), value);
    const auto offset =  m_header_offset + PageLayout::LSN_OFFSET;
    put_u64(*this, offset, value.value);
}

auto Page::id() const -> Id
{
    return m_id;
}

auto Page::size() const -> Size
{
    return m_data.size();
}

auto Page::view(Size offset) const -> Slice
{
    return m_data.range(offset);
}

auto Page::view(Size offset, Size size) const -> Slice
{
    return m_data.range(offset, size);
}

auto Page::read(Span out, Size offset) const -> void
{
    mem_copy(out, m_data.range(offset, out.size()));
}

auto Page::span(Size offset) -> Span
{
    return span(offset, size() - offset);
}

auto Page::span(Size offset, Size size) -> Span
{
    CALICO_EXPECT_TRUE(m_is_writable);
    insert_delta(m_deltas, PageDelta {offset, size});
    return m_data.range(offset, size);
}

auto Page::write(const Slice &in, Size offset) -> void
{
    mem_copy(span(offset, in.size()), in);
}

auto Page::apply_update(const FullImageDescriptor &info) -> void
{
    CALICO_EXPECT_EQ(m_id, info.pid);
    CALICO_EXPECT_EQ(m_data.size(), info.image.size());
    mem_copy(m_data, info.image);
}

auto Page::apply_update(const DeltaDescriptor &info) -> void
{
    CALICO_EXPECT_EQ(m_id, info.pid);
    for (const auto &[offset, delta]: info.deltas)
        mem_copy(m_data.range(offset, delta.size()), delta);
}

auto Page::collect_deltas() -> std::vector<PageDelta>
{
    const auto compressed_size = compress_deltas(m_deltas);
    const auto full_size = wal_scratch_size(m_data.size());
    if (compressed_size + DELTA_PAYLOAD_HEADER_SIZE > full_size)
        m_deltas = {{0, m_data.size()}};
    return m_deltas;
}

auto get_u16(const Page &page, Size offset) -> std::uint16_t
{
    return get_u16(page.view(offset, sizeof(std::uint16_t)));
}

auto get_u32(const Page &page, Size offset) -> std::uint32_t
{
    return get_u32(page.view(offset, sizeof(std::uint32_t)));
}

auto get_u64(const Page &page, Size offset) -> std::uint64_t
{
    return get_u64(page.view(offset, sizeof(std::uint64_t)));
}

auto put_u16(Page &page, Size offset, std::uint16_t value) -> void
{
    put_u16(page.span(offset, sizeof(value)), value);
}

auto put_u32(Page &page, Size offset, std::uint32_t value) -> void
{
    put_u32(page.span(offset, sizeof(value)), value);
}

auto put_u64(Page &page, Size offset, std::uint64_t value) -> void
{
    put_u64(page.span(offset, sizeof(value)), value);
}

} // namespace Calico
