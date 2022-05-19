
#ifndef CUB_POOL_FRAME_H
#define CUB_POOL_FRAME_H

#include <limits>

#include "common.h"
#include "utils/encoding.h"
#include "utils/layout.h"
#include "utils/slice.h"

namespace cub {

class IBufferPool;
class Page;

/**
 * Represents in-memory file for a single database page.
 */
class Frame {
public:
    friend class FrameReference;

    explicit Frame(Size);
    [[nodiscard]] auto page_lsn() const -> LSN;
    [[nodiscard]] auto page_id() const -> PID;
    [[nodiscard]] auto ref_count() const -> Size;
    [[nodiscard]] auto is_dirty() const -> bool;
    [[nodiscard]] auto data() const -> RefBytes;
    auto data() -> MutBytes;
    auto clean() -> void;
    auto reset(PID) -> void;
    auto borrow(IBufferPool *) -> Page;
    auto synchronize(bool) -> void;

private:
    std::unique_ptr<Byte[]> m_data;
    PID m_page_id {};
    Size m_ref_count {};
    Size m_size {};
    bool m_is_dirty {};
};

} // cub

#endif // CUB_POOL_FRAME_H
