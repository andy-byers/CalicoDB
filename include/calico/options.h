#ifndef CCO_OPTIONS_H
#define CCO_OPTIONS_H

#include "common.h"
#include <spdlog/spdlog.h>

namespace cco {

class Storage;
class WriteAheadLog;

static constexpr Size MINIMUM_FRAME_COUNT {0x8};
static constexpr Size DEFAULT_FRAME_COUNT {0x80};
static constexpr Size MAXIMUM_FRAME_COUNT {0x2000};
static constexpr Size MINIMUM_PAGE_SIZE {0x100};
static constexpr Size DEFAULT_PAGE_SIZE {0x8000};
static constexpr Size MAXIMUM_PAGE_SIZE {0x8000};
static constexpr auto DEFAULT_LOG_LEVEL = spdlog::level::off;

/**
 * Options to use when opening a database.
 */
struct Options {
    using LogLevel = spdlog::level::level_enum;

    Size page_size {DEFAULT_PAGE_SIZE}; ///< Size of a database page in bytes.
    Size frame_count {DEFAULT_FRAME_COUNT}; ///< Number of frames to allow the buffer pool cache.
    LogLevel log_level {DEFAULT_LOG_LEVEL}; ///< The max level of log message that will be written.
    Storage *store {};
    WriteAheadLog *wal {};
};

} // cco

#endif // CCO_OPTIONS_H
