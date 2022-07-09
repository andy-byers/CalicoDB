#ifndef CALICO_PAGE_PAGE_H
#define CALICO_PAGE_PAGE_H

#include <optional>
#include <vector>
#include "update.h"
#include "utils/scratch.h"
#include "utils/types.h"

namespace calico {

class IBufferPool;

class Page final {
public:
    struct Parameters {
        PID id;
        Bytes data;
        IBufferPool *source {};
        bool is_writable {};
        bool is_dirty {};
    };
    explicit Page(const Parameters&);
    ~Page();

    [[nodiscard]] auto is_writable() const -> bool
    {
        return m_is_writable;
    }

    [[nodiscard]] auto is_dirty() const -> bool
    {
        return m_is_dirty;
    }

    auto set_transient() -> void
    {
        m_is_transient = true;
    }

    [[nodiscard]] auto type() const -> PageType;
    [[nodiscard]] auto lsn() const -> LSN;
    auto set_type(PageType) -> void;

    auto set_lsn(LSN) -> void;
    [[nodiscard]] auto id() const -> PID;
    [[nodiscard]] auto size() const -> Size;
    [[nodiscard]] auto range(Index) const -> BytesView;
    [[nodiscard]] auto range(Index, Size) const -> BytesView;
    [[nodiscard]] auto get_u16(Index) const -> uint16_t;
    [[nodiscard]] auto get_u32(Index) const -> uint32_t;
    [[nodiscard]] auto has_changes() const -> bool;
    auto read(Bytes, Index) const -> void;
    auto mut_range(Index) -> Bytes;
    auto mut_range(Index, Size) -> Bytes;
    auto put_u16(Index, uint16_t) -> void;
    auto put_u32(Index, uint32_t) -> void;
    auto write(BytesView, Index) -> void;
    auto collect_changes() -> std::vector<ChangedRegion>;
    auto enable_tracking(Scratch) -> void;
    auto undo_changes(LSN, const std::vector<ChangedRegion>&) -> void;
    auto redo_changes(LSN, const std::vector<ChangedRegion>&) -> void;
    auto raw_data() -> Bytes;

    Page(Page&&) noexcept = default;
    auto operator=(Page&&) noexcept -> Page&;

private:
    [[nodiscard]] auto header_offset() const -> Index;
    auto do_release() noexcept -> void;
    auto do_change(Index, Size) -> void;

    Unique<IBufferPool*> m_pool;
    std::optional<UpdateManager> m_updates;
    Bytes m_data;
    PID m_id;
    bool m_is_transient {};
    bool m_is_writable {};
    bool m_is_dirty {};
};

} // calico

#endif // CALICO_PAGE_PAGE_H
