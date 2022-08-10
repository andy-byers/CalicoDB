#ifndef CCO_PAGE_UPDATE_H
#define CCO_PAGE_UPDATE_H

#include "page.h"
#include "utils/identifier.h"
#include "utils/scratch.h"
#include <optional>
#include <vector>

namespace cco {


class ChangeManager {
public:
    ChangeManager(BytesView, ManualScratch, ManualScratch);
    [[nodiscard]] auto collect_changes() -> std::vector<ChangedRegion>;
    auto release_scratches(ManualScratchManager&) -> void;
    auto push_change(PageChange) -> void;

    [[nodiscard]] auto has_changes() -> bool
    {
        return !m_changes.empty();
    }

private:
    std::vector<PageChange> m_changes;
    ManualScratch m_before;
    ManualScratch m_after;
    BytesView m_current;
};

namespace impl {
    auto can_merge(const PageChange &lhs, const PageChange &rhs) -> bool;
    auto merge(const PageChange &lhs, const PageChange &rhs) -> PageChange;
    auto compress_ranges(std::vector<PageChange> &ranges) -> void;
    auto insert_range(std::vector<PageChange> &ranges, PageChange) -> void;

} // namespace impl

} // namespace cco

#endif // CCO_PAGE_UPDATE_H