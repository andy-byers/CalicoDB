
#ifndef CCO_POOL_FRAME_H
#define CCO_POOL_FRAME_H

#include "calico/bytes.h"
#include "utils/identifier.h"
#include <memory>

namespace cco {

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
    Frame(Byte *, Index, Size);
    ~Frame() = default;

    [[nodiscard]] auto is_owned() const -> bool
    {
        return !m_owned.empty();
    }

    [[nodiscard]] auto page_id() const -> PageId
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

    auto reset(PageId page_id) -> void
    {
        CCO_EXPECT_EQ(m_ref_count, 0);
        m_page_id = page_id;
        m_is_dirty = false;
    }

    /**
     * Reset the reference count and flags.
     *
     * This method is used when we have lost a page, e.g. when a system call fails in one of the
     * tree balancing methods. Should generally be followed by a WAL roll back or program exit.
     */
    auto purge() -> void
    {
        m_ref_count = 0;
        m_is_writable = false;
        m_is_dirty = false;
    }

    [[nodiscard]] auto page_lsn() const -> SequenceNumber;
    [[nodiscard]] auto borrow(IBufferPool *, bool) -> Page;
    auto synchronize(Page &) -> void;

    auto operator=(Frame &&) -> Frame & = default;
    Frame(Frame &&) = default;

private:
    std::string m_owned;
    Bytes m_bytes;
    PageId m_page_id;
    Size m_ref_count {};
    Size m_size {};
    bool m_is_writable {};
    bool m_is_dirty {};
};

} // namespace cco

#endif // CCO_POOL_FRAME_H
