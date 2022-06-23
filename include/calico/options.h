#ifndef CALICO_OPTIONS_H
#define CALICO_OPTIONS_H

#include "common.h"

namespace calico {

static constexpr Size MIN_FRAME_COUNT {0x8};
static constexpr Size MAX_FRAME_COUNT {0x1000};
static constexpr Size MIN_PAGE_SIZE {0x100};
static constexpr Size MAX_PAGE_SIZE {1 << 15};
static constexpr Size MIN_BLOCK_SIZE {MIN_PAGE_SIZE};
static constexpr Size MAX_BLOCK_SIZE {MAX_PAGE_SIZE};
static constexpr Size DEFAULT_FRAME_COUNT {0x80};
static constexpr Size DEFAULT_PAGE_SIZE {0x4000};
static constexpr Size DEFAULT_BLOCK_SIZE {0x8000};
static constexpr bool DEFAULT_USE_TRANSACTIONS {true};
static constexpr bool DEFAULT_USE_DIRECT_IO {};
static constexpr int DEFAULT_PERMISSIONS {0666};

/**
 * Options to use when opening a database.
 */
struct Options {
    Size page_size {DEFAULT_PAGE_SIZE}; ///< Size of a database page in bytes.
    Size block_size {DEFAULT_BLOCK_SIZE}; ///< Size of a WAL block in bytes.
    Size frame_count {DEFAULT_FRAME_COUNT}; ///< Number of frames to allow the buffer pool.
    int permissions {DEFAULT_PERMISSIONS}; ///< Permissions with which to open files.
    bool use_transactions {DEFAULT_USE_TRANSACTIONS}; ///< True if we should use transactions, false otherwise.
    bool use_direct_io {DEFAULT_USE_DIRECT_IO}; ///< True if we should use direct I/O, false otherwise.
    std::string log_path; ///< Where to log error messages and other information.
    unsigned log_level {}; ///< The max level of log message that will be written.
};

} // calico

#endif // CALICO_OPTIONS_H
