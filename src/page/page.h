#ifndef CUB_PAGE_PAGE_H
#define CUB_PAGE_PAGE_H

#include <vector>
#include "common.h"
#include "utils/encoding.h"
#include "utils/layout.h"
#include "utils/scratch.h"
#include "utils/types.h"

namespace cub {

inline auto is_page_type_valid(PageType type) -> bool
{
    return type == PageType::INTERNAL_NODE ||
           type == PageType::EXTERNAL_NODE ||
           type == PageType::OVERFLOW_LINK ||
           type == PageType::FREELIST_LINK;
}

struct ChangedRegion {
    Index offset{};  ///< Offset of the region from the start of the page
    RefBytes before; ///< Contents of the region pre-update
    RefBytes after;  ///< Contents of the region post-update
};

struct PageUpdate {
    std::vector<ChangedRegion> changes;
    PID page_id{NULL_ID_VALUE};
    LSN previous_lsn{};
    LSN lsn{};
};

class IBufferPool;

class Page final {
public:
    struct Parameters {
        PID id;
        MutBytes data;
        IBufferPool *source {};
        bool is_writable {};
        bool is_dirty {};
    };
    Page(const Parameters&); // TODO: Parameters struct.
    ~Page();

    [[nodiscard]] auto is_writable() const -> bool {return m_is_writable;}
    [[nodiscard]] auto is_dirty() const -> bool {return m_is_dirty;}

    [[nodiscard]] auto type() const -> PageType;
    [[nodiscard]] auto lsn() const -> LSN;
    auto set_type(PageType) -> void;
    auto set_lsn(LSN) -> void;

    [[nodiscard]] auto id() const -> PID;
    [[nodiscard]] auto size() const -> Size;
    [[nodiscard]] auto range(Index) const -> RefBytes;
    [[nodiscard]] auto range(Index, Size) const -> RefBytes;
    [[nodiscard]] auto get_u16(Index) const -> uint16_t;
    [[nodiscard]] auto get_u32(Index) const -> uint32_t;
    auto read(MutBytes, Index) const -> void;
    auto mut_range(Index) -> MutBytes;
    auto mut_range(Index, Size) -> MutBytes;
    auto put_u16(Index, uint16_t) -> void;
    auto put_u32(Index, uint32_t) -> void;
    auto write(RefBytes, Index) -> void;
    [[nodiscard]] auto has_changes() const -> bool;
    auto collect_changes() -> std::vector<ChangedRegion>;
    auto enable_tracking(Scratch) -> void;
    auto undo_changes(LSN, const std::vector<ChangedRegion>&) -> void;
    auto redo_changes(LSN, const std::vector<ChangedRegion>&) -> void;
    auto raw_data() -> MutBytes;

    Page(Page&&) noexcept = default;
    auto operator=(Page&&) noexcept -> Page&;

private:
    auto do_release() noexcept -> void;
    auto do_change(Index, Size) -> void;
    auto header_offset() const -> Index;

    Unique<IBufferPool *> m_pool;
    std::optional<Scratch> m_snapshot;
    std::vector<ChangedRegion> m_changes;
    MutBytes m_data;
    PID m_id;
    bool m_is_writable{};
    bool m_is_dirty{};
};

} // cub

#endif // CUB_PAGE_PAGE_H
