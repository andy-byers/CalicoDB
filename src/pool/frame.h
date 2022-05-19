//
//#ifndef CUB_POOL_FRAME_H
//#define CUB_POOL_FRAME_H
//
//#include <limits>
//
//#include "common.h"
//#include "utils/encoding.h"
//#include "utils/layout.h"
//#include "utils/slice.h"
//
//namespace cub {
//
///**
// * Represents in-memory storage for a single database page.
// */
//class Frame {
//public:
//    friend class FrameReference;
//
//    explicit Frame(Size);
//    [[nodiscard]] auto page_lsn() const -> LSN;
//    [[nodiscard]] auto page_id() const -> PID {return m_page_id;}
//    [[nodiscard]] auto ref_count() const -> uint32_t {return m_ref_count.load();}
//    [[nodiscard]] auto is_dirty() const -> bool {return m_is_dirty;}
//    [[nodiscard]] auto data() const -> RefBytes {return {m_data.get(), m_size};}
//    auto data() -> MutBytes {return {m_data.get(), m_size};}
//    auto clean() -> void {m_is_dirty = false;} // TODO: Needed for BufferPool::purge(), but messes with encapsulation :(
//    auto reset(PID) -> void;
//    auto borrow_reference() -> std::unique_ptr<FrameReference>;
//    auto synchronize(FrameReference &reference) -> void;
//
//private:
////    mutable std::mutex m_mutex;
//    std::unique_ptr<Byte[]> m_data;
//    std::atomic<uint32_t> m_ref_count;
//    PID m_page_id{};
//    Size m_size{};
//    bool m_is_dirty{};
//};
//
//} // cub
//
//#endif // CUB_POOL_FRAME_H
