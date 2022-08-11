#include "page.h"
#include "pool/interface.h"
#include "utils/encoding.h"
#include "utils/layout.h"

namespace cco {

Page::Page(const Parameters &param)
    : m_source {param.source},
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
        [[maybe_unused]] auto s = m_source->release(std::move(*this));
}

auto Page::header_offset() const -> Index
{
    return PageLayout::header_offset(m_id);
}

auto Page::type() const -> PageType
{
    return PageType {get_u16(*this, header_offset() + PageLayout::TYPE_OFFSET)};
}

auto Page::lsn() const -> SequenceNumber
{
    return SequenceNumber {get_u64(*this, header_offset() + PageLayout::LSN_OFFSET)};
}

auto Page::set_type(PageType type) -> void
{
    const auto offset = header_offset() + PageLayout::TYPE_OFFSET;
    put_u16(*this, offset, static_cast<uint16_t>(type));
}

auto Page::set_lsn(SequenceNumber lsn) -> void
{
    const auto offset = header_offset() + PageLayout::LSN_OFFSET;
    put_u64(*this, offset, lsn.value);
}

auto Page::id() const -> PageId
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
    m_deltas.emplace_back(PageDelta {offset, size});
    m_is_dirty = true;
    return m_data.range(offset, size);
}

auto Page::write(BytesView in, Index offset) -> void
{
    mem_copy(bytes(offset, in.size()), in);
}

auto Page::undo(const UndoDescriptor &descriptor) -> void
{

    CCO_EXPECT_EQ(m_id, descriptor.page_id);
    CCO_EXPECT_EQ(m_data.size(), descriptor.image.size());
    mem_copy(m_data, descriptor.image);
    m_is_dirty = true; // Dirty flag is set here and in redo(), even though we don't have any deltas.
}

auto Page::redo(const RedoDescriptor &descriptor) -> void
{
    CCO_EXPECT_EQ(m_id, descriptor.page_id);
    CCO_EXPECT_FALSE(descriptor.is_commit);
    for (const auto &[offset, delta]: descriptor.deltas)
        mem_copy(m_data.range(offset, delta.size()), delta);
    m_is_dirty = true;
}

auto Page::deltas() const -> std::vector<PageDelta>
{
    return m_deltas;
}

auto get_u16(const Page &page, Index offset) -> std::uint16_t
{
    return get_u16(page.view(offset, sizeof(std::uint16_t)));
}

auto get_u32(const Page &page, Index offset) -> std::uint32_t
{
    return get_u32(page.view(offset, sizeof(std::uint32_t)));
}

auto get_u64(const Page &page, Index offset) -> std::uint64_t
{
    return get_u64(page.view(offset, sizeof(std::uint64_t)));
}

auto put_u16(Page &page, Index offset, std::uint16_t value) -> void
{
    put_u16(page.bytes(offset, sizeof(value)), value);
}

auto put_u32(Page &page, Index offset, std::uint32_t value) -> void
{
    put_u32(page.bytes(offset, sizeof(value)), value);
}

auto put_u64(Page &page, Index offset, std::uint64_t value) -> void
{
    put_u64(page.bytes(offset, sizeof(value)), value);
}

} // namespace cco
