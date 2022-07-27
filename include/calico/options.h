#ifndef CCO_OPTIONS_H
#define CCO_OPTIONS_H

#include "common.h"
#include <spdlog/spdlog.h>

namespace cco {

static constexpr Size MINIMUM_FRAME_COUNT {0x8};
static constexpr Size DEFAULT_FRAME_COUNT {0x80};
static constexpr Size MAXIMUM_FRAME_COUNT {0x2000};
static constexpr Size MINIMUM_PAGE_SIZE {0x100};
static constexpr Size DEFAULT_PAGE_SIZE {0x8000};
static constexpr Size MAXIMUM_PAGE_SIZE {0x8000};
static constexpr auto DEFAULT_USE_XACT = true;
static constexpr auto DEFAULT_LOG_LEVEL = spdlog::level::off;
static constexpr int DEFAULT_PERMISSIONS {0644}; ///< -rw-r--r--

/**
 * Options to use when opening a database.
 */
struct Options {
    std::string path; ///< Path at which to open the database (leave blank to open an in-memory database).
    Size page_size {DEFAULT_PAGE_SIZE}; ///< Size of a database page in bytes.
    Size frame_count {DEFAULT_FRAME_COUNT}; ///< Number of frames to allow the buffer pool.
    int permissions {DEFAULT_PERMISSIONS}; ///< Permissions with which to open files.
    bool use_xact {DEFAULT_USE_XACT};
    spdlog::level::level_enum log_level {DEFAULT_LOG_LEVEL}; ///< The max level of log message that will be written.
};

} // cco

#endif // CCO_OPTIONS_H
