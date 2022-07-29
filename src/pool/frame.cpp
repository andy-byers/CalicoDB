#include "frame.h"

#include "interface.h"
#include "page/page.h"
#include "utils/encoding.h"
#include "utils/layout.h"

namespace cco {

Frame::Frame(Size size)
    : m_owned(size, '\x00'),
      m_bytes {stob(m_owned)},
      m_size {size}
{
    CCO_EXPECT_TRUE(is_power_of_two(size));
    CCO_EXPECT_GE(size, MINIMUM_PAGE_SIZE);
    CCO_EXPECT_LE(size, MAXIMUM_PAGE_SIZE);
}

Frame::Frame(Byte *buffer, Index id, Size size)
    : m_bytes {buffer + id * size, size},
      m_size {size}
{
    CCO_EXPECT_TRUE(is_power_of_two(size));
    CCO_EXPECT_GE(size, MINIMUM_PAGE_SIZE);
    CCO_EXPECT_LE(size, MAXIMUM_PAGE_SIZE);
}

auto Frame::borrow(IBufferPool *pool, bool is_writable) -> Page
{
    CCO_EXPECT_FALSE(m_is_writable);

    if (is_writable) {
        CCO_EXPECT_EQ(m_ref_count, 0);
        m_is_writable = true;
    }
    m_ref_count++;
    return Page {{m_page_id, data(), pool, is_writable, m_is_dirty}};
}

auto Frame::synchronize(Page &page) -> void
{
    CCO_EXPECT_GT(m_ref_count, 0);

    if (page.is_writable()) {
        CCO_EXPECT_EQ(m_ref_count, 1);
        m_is_writable = false;
    }
    if (page.is_dirty()) {
        m_is_dirty = true;
        page.m_is_dirty = false;
    }
    // Make sure the page doesn't get released twice.
    page.m_source.reset();
    m_ref_count--;
}

auto Frame::page_lsn() const -> LSN
{
    return LSN {get_u32(m_bytes.range(PageLayout::header_offset(m_page_id) + PageLayout::LSN_OFFSET))};
}

} // namespace cco