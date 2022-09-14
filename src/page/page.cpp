#include "page.h"
#include "deltas.h"
#include "pager/pager.h"
#include "utils/encoding.h"
#include "utils/layout.h"

namespace calico {

Page::Page(const Parameters &param)
    : m_source {param.source},
      m_data {param.data},
      m_id {param.id},
      m_is_writable {param.is_writable},
      m_is_dirty {param.is_dirty}
{
    CALICO_EXPECT_TRUE(is_power_of_two(m_data.size()));
    CALICO_EXPECT_GE(m_data.size(), MINIMUM_PAGE_SIZE);
    CALICO_EXPECT_LE(m_data.size(), MAXIMUM_PAGE_SIZE);
}

Page::~Page()
{
    if (m_source.is_valid()) {
        const auto source = std::move(m_source);
        [[maybe_unused]] auto s = source->release(std::move(*this));
    }
}

auto Page::header_offset() const -> Size
{
    return PageLayout::header_offset(m_id);
}

auto Page::type() const -> PageType
{
    return PageType {get_u16(*this, header_offset() + PageLayout::TYPE_OFFSET)};
}

auto Page::lsn() const -> SequenceId
{
    return SequenceId {get_u64(*this, header_offset() + PageLayout::LSN_OFFSET)};
}

auto Page::set_type(PageType type) -> void
{
    const auto offset = header_offset() + PageLayout::TYPE_OFFSET;
    put_u16(*this, offset, static_cast<uint16_t>(type));
}

auto Page::set_lsn(SequenceId value) -> void
{
    CALICO_EXPECT_LE(lsn(), value);
    const auto offset = header_offset() + PageLayout::LSN_OFFSET;
    put_u64(*this, offset, value.value);
}

auto Page::id() const -> PageId
{
    return m_id;
}

auto Page::size() const -> Size
{
    return m_data.size();
}

auto Page::view(Size offset) const -> BytesView
{
    return m_data.range(offset);
}

auto Page::view(Size offset, Size size) const -> BytesView
{
    return m_data.range(offset, size);
}

auto Page::read(Bytes out, Size offset) const -> void
{
    mem_copy(out, m_data.range(offset, out.size()));
}

auto Page::bytes(Size offset) -> Bytes
{
    return bytes(offset, size() - offset);
}

auto Page::bytes(Size offset, Size size) -> Bytes
{
    CALICO_EXPECT_TRUE(m_is_writable);
    insert_delta(m_deltas, PageDelta {offset, size});
    m_is_dirty = true;
    return m_data.range(offset, size);
}

auto Page::write(BytesView in, Size offset) -> void
{
    mem_copy(bytes(offset, in.size()), in);
}

auto Page::apply_update(const FullImageDescriptor &info) -> void
{
    CALICO_EXPECT_EQ(m_id, info.pid);
    CALICO_EXPECT_EQ(m_data.size(), info.image.size());
    mem_copy(m_data, info.image);
    m_is_dirty = true; // Dirty flag is set here and in redo(), even though we don't have any deltas.
}

auto Page::apply_update(const DeltasDescriptor &info) -> void
{
    CALICO_EXPECT_EQ(m_id, info.pid);
    if (lsn() < info.lsn) {
        for (const auto &[offset, delta]: info.deltas)
            mem_copy(m_data.range(offset, delta.size()), delta);
        m_is_dirty = true;
    }
}

auto Page::collect_deltas() -> std::vector<PageDelta>
{
    compress_deltas(m_deltas);
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
    put_u16(page.bytes(offset, sizeof(value)), value);
}

auto put_u32(Page &page, Size offset, std::uint32_t value) -> void
{
    put_u32(page.bytes(offset, sizeof(value)), value);
}

auto put_u64(Page &page, Size offset, std::uint64_t value) -> void
{
    put_u64(page.bytes(offset, sizeof(value)), value);
}

} // namespace calico
