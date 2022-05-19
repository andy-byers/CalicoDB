//
//#include "frame.h"
//#include "layout.h"
//#include "page.h"
//#include "utils.h"
//
//namespace cub {
//
//Frame::Frame(Size size)
//    : m_data{std::unique_ptr<Byte[]>{new(static_cast<std::align_val_t>(size)) Byte[size]}}
//    , m_size{size}
//{
//    CUB_EXPECT_TRUE(is_power_of_two(size));
//    CUB_EXPECT_EQ(reinterpret_cast<std::uintptr_t>(m_data.get()) % size, 0);
//    mem_clear(data());
//}
//
//auto Frame::reset(PID page_id) -> void
//{
//    CUB_EXPECT_EQ(m_ref_count.load(), 0);
//    m_page_id = page_id;
//    m_is_dirty = false;
//}
//
//auto Frame::borrow_reference() -> std::unique_ptr<FrameReference>
//{
//    m_ref_count.fetch_add(1);
//    return std::unique_ptr<FrameReference>{new FrameReference{*this}};
//}
//
//auto Frame::synchronize(FrameReference &reference) -> void
//{
//    CUB_EXPECT_GT(m_ref_count.load(), 0);
//    CUB_EXPECT_EQ(m_page_id, reference.m_id);
//    m_ref_count.fetch_sub(1);
//}
//
//auto Frame::page_lsn() const -> LSN
//{
//    return PageHeader{m_page_id, data()}.lsn();
//}
//
//} // cub