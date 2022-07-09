
#ifndef CALICO_POOL_FRAME_H
#define CALICO_POOL_FRAME_H

#include <memory>
#include "calico/bytes.h"
#include "utils/identifier.h"

namespace calico {

class IBufferPool;
class Page;

/**
 * Represents in-memory storage for a single database page.
 */
class Frame final {
public:
    using Reference = std::reference_wrapper<Frame>;
    using ConstReference = std::reference_wrapper<const Frame>;

    explicit Frame(Size);
    Frame(Byte*, Index, Size);
    ~Frame() = default;

    [[nodiscard]] auto is_owned() const -> bool
    {
        return !m_owned.empty();
    }

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

    [[nodiscard]] auto size() const -> Size
    {
        return m_size;
    }

    [[nodiscard]] auto data() const -> BytesView
    {
        return m_bytes;
    }

    auto data() -> Bytes
    {
        return m_bytes;
    }

    auto clean() -> void
    {
        m_is_dirty = false;
    }

    auto reset(PID page_id) -> void
    {
        CALICO_EXPECT_EQ(m_ref_count, 0);
        m_page_id = page_id;
        m_is_dirty = false;
    }

    [[nodiscard]] auto page_lsn() const -> LSN;
    auto borrow(IBufferPool*, bool) -> Page;
    auto synchronize(Page&) -> void;

    auto operator=(Frame&&) -> Frame& = default;
    Frame(Frame&&) = default;

private:
    std::string m_owned;
    Bytes m_bytes;
    PID m_page_id {};
    Size m_ref_count {};
    Size m_size {};
    bool m_is_writable {};
    bool m_is_dirty {};
};

} // calico

#endif // CALICO_POOL_FRAME_H
