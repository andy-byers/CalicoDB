
#include "frame.h"
#include "interface.h"
#include "page/page.h"
#include "utils/encoding.h"
#include "utils/layout.h"

namespace cub {

Frame::Frame(Size size)
    : m_data {std::unique_ptr<Byte[], AlignedDeleter> {
          new(static_cast<std::align_val_t>(size)) Byte[size],
          AlignedDeleter {static_cast<std::align_val_t>(size)}}}
    , m_size {size}
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

auto Frame::data() const -> BytesView
{
    return {m_data.get(), m_size};
}

auto Frame::data() -> Bytes
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

auto Frame::borrow(IBufferPool *parent, bool is_writable) -> Page
{
    CUB_EXPECT_FALSE(m_is_writable);

    if (is_writable) {
        CUB_EXPECT_EQ(m_ref_count, 0);
        m_is_writable = true;
    }
    m_ref_count++;
    return Page{{m_page_id, data(), parent, is_writable, m_is_dirty}};
}

auto Frame::synchronize(Page &page) -> void
{
    CUB_EXPECT_GT(m_ref_count, 0);

    if (page.is_writable()) {
        CUB_EXPECT_EQ(m_ref_count, 1);
        m_is_writable = false;
    }

    if (page.is_dirty())
        m_is_dirty = true;

    m_ref_count--;
}

auto Frame::page_lsn() const -> LSN
{
    const auto offset = PageLayout::header_offset(m_page_id) + PageLayout::LSN_OFFSET;
    return LSN {get_uint32(data().range(offset))};
}

} // cub