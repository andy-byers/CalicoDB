
#include "frame.h"
#include "interface.h"
#include "page/page.h"
#include "utils/layout.h"

namespace cub {

Frame::Frame(Size size)
    : m_data{std::unique_ptr<Byte[]>{new(static_cast<std::align_val_t>(size)) Byte[size]}}
    , m_size{size}
{
    CUB_EXPECT_TRUE(is_power_of_two(size));
    CUB_EXPECT_EQ(reinterpret_cast<std::uintptr_t>(m_data.get()) % size, 0);
    mem_clear(data());
}

auto Frame::page_id() const -> PID
{
    return m_page_id;
}

auto Frame::ref_count() const -> Size
{
    return m_ref_count;
}

auto Frame::is_dirty() const -> bool
{
    return m_is_dirty;
}

auto Frame::data() const -> RefBytes
{
    return {m_data.get(), m_size};
}

auto Frame::data() -> MutBytes
{
    return {m_data.get(), m_size};
}

auto Frame::clean() -> void
{
    m_is_dirty = false;
}

auto Frame::reset(PID page_id) -> void
{
    CUB_EXPECT_EQ(m_ref_count, 0);
    m_page_id = page_id;
    m_is_dirty = false;
}

auto Frame::borrow(IBufferPool *parent) -> Page
{
    m_ref_count++;
    return {m_page_id, data(), parent};
}

auto Frame::synchronize(bool was_dirty) -> void
{
    CUB_EXPECT_GT(m_ref_count, 0);
    m_ref_count--;

    // TODO: Currently Page doesn't accept the current dirty flag state in its
    //       constructor, so we can only tell if the page was made dirty since
    //       it was last acquired by looking at the Page instance. We won't know
    //       if the page was already dirty. This is okay right now, but may need
    //       to be changed.
    if (was_dirty)
        m_is_dirty = true;
}

auto Frame::page_lsn() const -> LSN
{
    const auto offset = PageLayout::header_offset(m_page_id) + PageLayout::LSN_OFFSET;
    return LSN {get_uint32(data().range(offset))};
}

} // cub