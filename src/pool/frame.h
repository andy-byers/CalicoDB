
#ifndef CCO_POOL_FRAME_H
#define CCO_POOL_FRAME_H

#include "calico/bytes.h"
#include "utils/identifier.h"
#include <memory>
#include <list>

namespace cco {

class IBufferPool;
class Page;

using FrameId = Identifier<std::uint64_t>;

struct Pin {
    FrameId fid;
    PageId pid;
};

/**
 * Represents in-memory storage for a single database page.
 */
class Frame final {
public:
    explicit Frame(Size);
    Frame(Byte *, Index, Size);

    [[nodiscard]]
    auto pid() const -> PageId
    {
        return m_page_id;
    }

    [[nodiscard]]
    auto ref_count() const -> Size
    {
        return m_ref_count;
    }

    [[nodiscard]]
    auto is_dirty() const -> bool
    {
        return m_is_dirty;
    }

    [[nodiscard]]
    auto data() const -> BytesView
    {
        return m_bytes;
    }

    [[nodiscard]]
    auto data() -> Bytes
    {
        return m_bytes;
    }

    auto reset(PageId id) -> void
    {
        CCO_EXPECT_EQ(m_ref_count, 0);
        m_page_id = id;
        m_is_dirty = false;
    }

    [[nodiscard]] auto ref(IBufferPool*, bool) -> Page;
    auto unref(Page &page) -> void;

private:
    Bytes m_bytes;
    PageId m_page_id;
    Size m_ref_count {};
    bool m_is_writable {};
    bool m_is_dirty {};
};

} // namespace cco

#endif // CCO_POOL_FRAME_H
