#ifndef CALICO_OPTIONS_H
#define CALICO_OPTIONS_H

#include "common.h"
#include <spdlog/spdlog.h>

namespace calico {

class Storage;
class WriteAheadLog;

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
/**
 * Options to use when opening a database.
 */
struct Options {
    using LogLevel = spdlog::level::level_enum;

    Size page_size {DEFAULT_PAGE_SIZE}; ///< Size of a database page in bytes.
    Size frame_count {DEFAULT_FRAME_COUNT}; ///< Number of frames to allow the block pool cache.
    Size wal_limit {DEFAULT_WAL_LIMIT}; ///< Number of blocks in a WAL segment.
    LogLevel log_level {DEFAULT_LOG_LEVEL}; ///< The max level of log message that will be written.
    Storage *store {};
};

/**
 * File header for populating the state of custom database components.
 */
struct FileHeader {
    std::uint32_t magic_code;
    std::uint32_t header_crc;
    std::uint64_t page_count;
    std::uint64_t freelist_head;
    std::uint64_t record_count;
    std::uint64_t flushed_lsn;
    std::uint16_t page_size;
    Byte reserved[6];
};

} // cco

#endif // CALICO_OPTIONS_H
