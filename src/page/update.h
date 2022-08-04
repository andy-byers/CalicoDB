#ifndef CCO_PAGE_UPDATE_H
#define CCO_PAGE_UPDATE_H

#include "page.h"
#include "utils/identifier.h"
#include "utils/scratch.h"
#include <optional>
#include <vector>

namespace cco {

struct ChangeDescriptor {
    Index offset {};
    Size size {};
};

struct ChangedRegion {
    Index offset {};  ///< Offset of the region from the start of the page
    BytesView before; ///< Contents of the region pre-update
    BytesView after;  ///< Contents of the region post-update
};

struct PageUpdate {
    std::vector<ChangedRegion> changes;
    PageId page_id;
    SequenceNumber previous_lsn;
    SequenceNumber lsn;
};

class ChangeManager {
public:
    ChangeManager(BytesView, ManualScratch, ManualScratch);
    [[nodiscard]] auto collect_changes() -> std::vector<ChangedRegion>;
    auto release_scratches(ManualScratchManager&) -> void;
    auto push_change(ChangeDescriptor) -> void;

    [[nodiscard]] auto has_changes() -> bool
    {
        return !m_changes.empty();
    }

private:
    std::vector<ChangeDescriptor> m_changes;
    ManualScratch m_before;
    ManualScratch m_after;
    BytesView m_current;
};

namespace impl {
    auto can_merge(const ChangeDescriptor &lhs, const ChangeDescriptor &rhs) -> bool;
    auto merge(const ChangeDescriptor &lhs, const ChangeDescriptor &rhs) -> ChangeDescriptor;
    auto compress_ranges(std::vector<ChangeDescriptor> &ranges) -> void;
    auto insert_range(std::vector<ChangeDescriptor> &ranges, ChangeDescriptor) -> void;

} // namespace impl

} // namespace cco

#endif // CCO_PAGE_UPDATE_H