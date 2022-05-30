#include "page.h"
#include "pool/interface.h"
#include "utils/encoding.h"
#include "utils/layout.h"

namespace cub {

Page::Page(const Parameters &param)
    : m_pool {param.source}
    , m_data {param.data}
    , m_id {param.id}
    , m_is_writable {param.is_writable}
    , m_is_dirty {param.is_dirty} {}

Page::~Page()
{
    do_release();
}

auto Page::operator=(Page &&rhs) noexcept -> Page&
{
    if (this != &rhs) {
        do_release();
        m_pool = std::move(rhs.m_pool);
        m_updates = std::exchange(rhs.m_updates, {});
        m_data = std::exchange(rhs.m_data, {});
        m_id = std::exchange(rhs.m_id, {});
        m_is_dirty = std::exchange(rhs.m_is_dirty, false);
    }
    return *this;
}

auto Page::do_release() noexcept -> void
{
    try {
        if (m_pool.value)
            m_pool.value->on_page_release(*this);

    } catch (...) {
        CUB_EXPECT_TRUE(!!m_pool.value);
        m_pool.value->on_page_error();
    }
}

auto Page::header_offset() const -> Index
{
    return PageLayout::header_offset(m_id);
}

auto Page::type() const -> PageType
{
    return static_cast<PageType>(get_u16(header_offset() + PageLayout::TYPE_OFFSET));
}

auto Page::lsn() const -> LSN
{
    return LSN{get_u32(header_offset() + PageLayout::LSN_OFFSET)};
}

auto Page::set_type(PageType type) -> void
{
    const auto offset = header_offset() + PageLayout::TYPE_OFFSET;
    do_change(offset, sizeof(uint16_t));
    put_uint16(m_data.range(offset), static_cast<uint16_t>(type));
}

auto Page::set_lsn(LSN lsn) -> void
{
    const auto offset = header_offset() + PageLayout::LSN_OFFSET;
    do_change(offset, sizeof(lsn.value));
    put_uint32(m_data.range(offset), lsn.value);
}

auto Page::id() const -> PID
{
    return m_id;
}

auto Page::size() const -> Size
{
    return m_data.size();
}

auto Page::range(Index offset) const -> BytesView
{
    return m_data.range(offset);
}

auto Page::range(Index offset, Size size) const -> BytesView
{
    return m_data.range(offset, size);
}

auto Page::get_u16(Index offset) const -> uint16_t
{
    return get_uint16(m_data.range(offset, sizeof(uint16_t)));
}

auto Page::get_u32(Index offset) const -> uint32_t
{
    return get_uint32(m_data.range(offset, sizeof(uint32_t)));
}

auto Page::read(Bytes out, Index offset) const -> void
{
    mem_copy(out, m_data.range(offset, out.size()));
}

auto Page::mut_range(Index offset) -> Bytes
{
    do_change(offset, size() - offset);
    return m_data.range(offset);
}

auto Page::mut_range(Index offset, Size size) -> Bytes
{
    do_change(offset, size);
    return m_data.range(offset, size);
}

auto Page::put_u16(Index offset, uint16_t value) -> void
{
    do_change(offset, sizeof(value));
    put_uint16(m_data.range(offset, sizeof(value)), value);
}

auto Page::put_u32(Index offset, uint32_t value) -> void
{
    do_change(offset, sizeof(value));
    put_uint32(m_data.range(offset, sizeof(value)), value);
}

auto Page::write(BytesView in, Index offset) -> void
{
    mem_copy(m_data.range(offset, in.size()), in);
}

auto Page::raw_data() -> Bytes
{
    return m_data;
}

auto Page::has_changes() const -> bool
{
    return m_updates && m_updates->has_changes();
}

auto Page::collect_changes() -> std::vector<ChangedRegion>
{
    if (m_updates)
        return m_updates->collect_changes(m_data);
    return {};
}

auto Page::enable_tracking(Scratch scratch) -> void
{
    CUB_EXPECT_EQ(scratch.size(), m_data.size());
    mem_copy(scratch.data(), m_data);
    m_updates = UpdateManager {std::move(scratch)};
}

auto Page::undo_changes(LSN previous_lsn, const std::vector<ChangedRegion> &changes) -> void
{
    CUB_EXPECT_GE(lsn(), previous_lsn);
    for (const auto &region: changes)
        mem_copy(m_data.range(region.offset), region.before, region.before.size());
    set_lsn(previous_lsn);
}

auto Page::redo_changes(LSN next_lsn, const std::vector<ChangedRegion> &changes) -> void
{
    CUB_EXPECT_LT(lsn(), next_lsn);
    for (const auto &region: changes)
        mem_copy(m_data.range(region.offset), region.after, region.after.size());
    set_lsn(next_lsn);
}

auto Page::do_change(Index offset, Size size) -> void
{
    CUB_EXPECT_TRUE(m_is_writable);
    CUB_EXPECT_GT(size, 0);
    CUB_EXPECT_LE(offset + size, Page::size());

    m_is_dirty = true;

    if (m_updates)
        m_updates->indicate_change(offset, size);
}

} // cub
