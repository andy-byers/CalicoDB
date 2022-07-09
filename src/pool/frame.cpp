#include "frame.h"

#include "interface.h"
#include "page/page.h"
#include "utils/encoding.h"
#include "utils/layout.h"

namespace calico {

Frame::Frame(Size size)
    : m_owned(size, '\x00'),
      m_bytes {stob(m_owned)},
      m_size {size}
{
    CALICO_EXPECT_TRUE(is_power_of_two(size));
    CALICO_EXPECT_GE(size, MINIMUM_PAGE_SIZE);
    CALICO_EXPECT_LE(size, MAXIMUM_PAGE_SIZE);
}

Frame::Frame(Byte *buffer, Index id, Size size)
    : m_bytes {buffer + id*size, size},
      m_size {size}
{
    CALICO_EXPECT_TRUE(is_power_of_two(size));
    CALICO_EXPECT_GE(size, MINIMUM_PAGE_SIZE);
    CALICO_EXPECT_LE(size, MAXIMUM_PAGE_SIZE);
}

auto Frame::borrow(IBufferPool *parent, bool is_writable) -> Page
{
    CALICO_EXPECT_FALSE(m_is_writable);

    if (is_writable) {
        CALICO_EXPECT_EQ(m_ref_count, 0);
        m_is_writable = true;
    }
    m_ref_count++;
    return Page{{m_page_id, data(), parent, is_writable, m_is_dirty}};
}

auto Frame::synchronize(Page &page) -> void
{
    CALICO_EXPECT_GT(m_ref_count, 0);

    if (page.is_writable()) {
        CALICO_EXPECT_EQ(m_ref_count, 1);
        m_is_writable = false;
    }
if (m_is_writable){

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

} // calico