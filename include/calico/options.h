#ifndef CALICO_OPTIONS_H
#define CALICO_OPTIONS_H

#include "common.h"
#include <spdlog/spdlog.h>

namespace calico {

class Storage;

static constexpr Size MINIMUM_FRAME_COUNT {0x8};
static constexpr Size DEFAULT_FRAME_COUNT {0x80};
static constexpr Size MAXIMUM_FRAME_COUNT {0x2000};
static constexpr Size MINIMUM_PAGE_SIZE {0x100};
static constexpr Size DEFAULT_PAGE_SIZE {0x2000};
static constexpr Size MAXIMUM_PAGE_SIZE {0x10000};
static constexpr auto DEFAULT_LOG_LEVEL = spdlog::level::off;
static constexpr auto MAXIMUM_LOG_LEVEL = spdlog::level::n_levels - 1;
static constexpr Size MINIMUM_WAL_LIMIT {0x20};
static constexpr Size DEFAULT_WAL_LIMIT {0x200};
static constexpr Size MAXIMUM_WAL_LIMIT {0x2000};
static constexpr Size DISABLE_WAL {0};

struct Options {
    using LogLevel = spdlog::level::level_enum;

    Size page_size {DEFAULT_PAGE_SIZE};
    Size frame_count {DEFAULT_FRAME_COUNT};
    Size wal_limit {DEFAULT_WAL_LIMIT};
    std::string_view wal_path;
    LogLevel log_level {DEFAULT_LOG_LEVEL};
    // std::string_view log_path; // TODO: Rotating log files...
    Storage *store {};
};

} // namespace calico

#endif // CALICO_OPTIONS_H
