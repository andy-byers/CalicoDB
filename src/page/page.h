#ifndef CCO_PAGE_PAGE_H
#define CCO_PAGE_PAGE_H

//#include "update.h"
#include "utils/identifier.h"
#include "utils/types.h"
#include <optional>
#include <vector>

namespace cco {

class IBufferPool;
class Frame;

namespace page {

struct ChangedRegion;
struct PageUpdate;
class UpdateManager;

class Page final {
public:
    friend class cco::Frame;

    struct Parameters {
        PID id;
        Bytes data;
        IBufferPool *source {};
        bool is_writable {};
        bool is_dirty {};
    };

    ~Page();
    explicit Page(const Parameters&);

    [[nodiscard]] auto is_writable() const -> bool
    {
        return m_is_writable;
    }

    [[nodiscard]] auto is_dirty() const -> bool
    {
        return m_is_dirty;
    }

    [[nodiscard]] auto id() const -> PID;
    [[nodiscard]] auto size() const -> Size;
    [[nodiscard]] auto view(Index) const -> BytesView;
    [[nodiscard]] auto view(Index, Size) const -> BytesView;
    [[nodiscard]] auto type() const -> PageType;
    [[nodiscard]] auto lsn() const -> LSN;
    auto set_type(PageType) -> void;
    auto set_lsn(LSN) -> void;
    auto read(Bytes, Index) const -> void;
    auto bytes(Index) -> Bytes;
    auto bytes(Index, Size) -> Bytes;
    auto write(BytesView, Index) -> void;
    auto undo(LSN, const std::vector<ChangedRegion>&) -> void;
    auto redo(LSN, const std::vector<ChangedRegion>&) -> void;

    [[nodiscard]] auto has_manager() const -> bool
    {
        return m_manager != nullptr;
    }

    auto set_manager(UpdateManager &manager) -> void
    {
        CCO_EXPECT_EQ(m_manager, nullptr);
        m_manager = &manager;
    }

    auto clear_manager() -> void
    {
        CCO_EXPECT_NE(m_manager, nullptr);
        m_manager = nullptr;
    }

    Page(Page&&) noexcept = default;
    auto operator=(Page&&) noexcept -> Page& = default;
    Page(const Page&) noexcept = delete;
    auto operator=(const Page&) noexcept -> Page& = delete;

private:
    [[nodiscard]] auto header_offset() const -> Index;

    utils::UniqueNullable<IBufferPool*> m_source;
    UpdateManager *m_manager {};
    Bytes m_data;
    PID m_id;
    bool m_is_writable {};
    bool m_is_dirty {};
};

[[nodiscard]] auto get_u16(const Page&, Index) -> uint16_t;
[[nodiscard]] auto get_u32(const Page&, Index) -> uint32_t;
auto put_u16(Page&, Index, uint16_t) -> void;
auto put_u32(Page&, Index, uint32_t) -> void;

} // page
} // cco

#endif // CCO_PAGE_PAGE_H
