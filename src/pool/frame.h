
#ifndef CUB_POOL_FRAME_H
#define CUB_POOL_FRAME_H

#include <memory>
#include "cub/bytes.h"
#include "utils/identifier.h"

namespace cub {

class IBufferPool;
class Page;

/**
 * Represents in-memory file for a single database page.
 */
class Frame final {
public:
    explicit Frame(Size);
    ~Frame() = default;
    [[nodiscard]] auto page_lsn() const -> LSN;
    [[nodiscard]] auto page_id() const -> PID;
    [[nodiscard]] auto ref_count() const -> Size;
    [[nodiscard]] auto is_dirty() const -> bool;
    [[nodiscard]] auto data() const -> BytesView;
    auto data() -> Bytes;
    auto clean() -> void;
    auto reset(PID) -> void;
    auto borrow(IBufferPool*, bool) -> Page;
    auto synchronize(Page&) -> void;

    auto operator=(Frame&&) -> Frame& = default;
    Frame(Frame&&) = default;

private:
    struct AlignedDeleter {

        explicit AlignedDeleter(std::align_val_t alignment)
            : align {alignment} {}

        auto operator()(Byte *ptr) const -> void
        {
            operator delete[](ptr, align);
        }

        std::align_val_t align;
    };

    std::unique_ptr<Byte[], AlignedDeleter> m_data;
    PID m_page_id {};
    Size m_ref_count {};
    Size m_size {};
    bool m_is_writable {};
    bool m_is_dirty {};
};

} // cub

#endif // CUB_POOL_FRAME_H
