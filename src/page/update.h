#ifndef CUB_PAGE_UPDATE_H
#define CUB_PAGE_UPDATE_H

#include <optional>
#include <vector>
#include "cub/bytes.h"
#include "cub/common.h"
#include "utils/identifier.h"
#include "utils/scratch.h"

namespace cub {

struct ChangedRegion {
    Index offset {};  ///< Offset of the region from the start of the page
    BytesView before; ///< Contents of the region pre-update
    BytesView after;  ///< Contents of the region post-update
};

struct PageUpdate {
    std::vector<ChangedRegion> changes;
    PID page_id {NULL_ID_VALUE};
    LSN previous_lsn {};
    LSN lsn {};
};

class UpdateManager {
public:
    struct Range {
        Index x {};
        Size dx {};
    };

    explicit UpdateManager(Scratch);
    [[nodiscard]] auto has_changes() const -> bool;
    [[nodiscard]] auto collect_changes(BytesView) -> std::vector<ChangedRegion>;
    auto indicate_change(Index, Size) -> void;

private:
    std::vector<Range> m_ranges;
    Scratch m_snapshot;
};

namespace test {

    auto update_basic_assertions() -> int;

} // test

} // cub

#endif // CUB_PAGE_UPDATE_H
