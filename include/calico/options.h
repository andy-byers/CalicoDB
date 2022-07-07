#ifndef CALICO_OPTIONS_H
#define CALICO_OPTIONS_H

#include "common.h"

namespace calico {

static constexpr Size MINIMUM_FRAME_COUNT {0x8};
static constexpr Size MAXIMUM_FRAME_COUNT {0x2000};
static constexpr Size MINIMUM_PAGE_SIZE {0x100};
static constexpr Size MAXIMUM_PAGE_SIZE {1 << 15};
static constexpr Size MINIMUM_BLOCK_SIZE {MINIMUM_PAGE_SIZE};
static constexpr Size MAXIMUM_BLOCK_SIZE {MAXIMUM_PAGE_SIZE};
static constexpr Size DEFAULT_FRAME_COUNT {0x80};
static constexpr Size DEFAULT_PAGE_SIZE {0x4000};
static constexpr Size DEFAULT_BLOCK_SIZE {0x8000};
static constexpr bool DEFAULT_USE_TRANSACTIONS {true};
static constexpr unsigned DEFAULT_LOG_LEVEL {};
static constexpr int DEFAULT_PERMISSIONS {0666};

/**
 * Options to use when opening a database.
 */
struct Options {
    Size page_size {DEFAULT_PAGE_SIZE}; ///< Size of a database page in bytes.
    Size block_size {DEFAULT_BLOCK_SIZE}; ///< Size of a WAL block in bytes.
    Size frame_count {DEFAULT_FRAME_COUNT}; ///< Number of frames to allow the buffer pool.
    unsigned log_level {DEFAULT_LOG_LEVEL}; ///< The max level of log message that will be written.
    int permissions {DEFAULT_PERMISSIONS}; ///< Permissions with which to open files.
    bool use_transactions {DEFAULT_USE_TRANSACTIONS}; ///< True if we should use transactions, false otherwise.
};

} // calico

#endif // CALICO_OPTIONS_H
