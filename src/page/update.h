#ifndef CCO_PAGE_UPDATE_H
#define CCO_PAGE_UPDATE_H

#include "page.h"
#include "utils/identifier.h"
#include "utils/scratch.h"
#include <optional>
#include <vector>

namespace cco::page {

struct ChangedRegion {
    Index offset {};  ///< Offset of the region from the start of the page
    BytesView before; ///< Contents of the region pre-update
    BytesView after;  ///< Contents of the region post-update
};

struct PageUpdate {
    std::vector<ChangedRegion> changes;
    PID page_id;
    LSN previous_lsn;
    LSN lsn;
};

class UpdateManager {
public:
    struct Range {
        Index x {};
        Size dx {};
    };

    UpdateManager(Page&, Bytes);
    [[nodiscard]] auto collect() -> std::vector<ChangedRegion>;
    auto push(Range) -> void;

private:
    std::vector<Range> m_ranges;
    BytesView m_snapshot;
    BytesView m_current;
};

namespace impl {

    using Range = UpdateManager::Range;

    auto can_merge(const Range &lhs, const Range &rhs) -> bool;
    auto merge(const Range &lhs, const Range &rhs) -> Range;
    auto compress_ranges(std::vector<Range> &ranges) -> void;
    auto insert_range(std::vector<Range> &ranges, Range) -> void;

} // impl

} // cco::page

#endif // CCO_PAGE_UPDATE_H