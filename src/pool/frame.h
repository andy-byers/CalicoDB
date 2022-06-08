
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

    [[nodiscard]] auto page_id() const -> PID
    {
        return m_page_id;
    }

    [[nodiscard]] auto ref_count() const -> Size
    {
        return m_ref_count;
    }

    [[nodiscard]] auto is_dirty() const -> bool
    {
        return m_is_dirty;
    }

    [[nodiscard]] auto data() const -> BytesView
    {
        return {m_data.get(), m_size};
    }

    auto data() -> Bytes
    {
        return {m_data.get(), m_size};
    }

    auto clean() -> void
    {
        m_is_dirty = false;
    }

    auto reset(PID page_id) -> void
    {
        CUB_EXPECT_EQ(m_ref_count, 0);
        m_page_id = page_id;
        m_is_dirty = false;
    }

    [[nodiscard]] auto page_lsn() const -> LSN;
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
