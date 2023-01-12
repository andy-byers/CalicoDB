#ifndef CALICO_OPTIONS_H
#define CALICO_OPTIONS_H

#include "slice.h"

namespace Calico {

class Storage;

static constexpr Size DEFAULT_CACHE_SIZE {0x100000}; // 1 MiB
static constexpr Size MINIMUM_PAGE_SIZE {0x100};
static constexpr Size DEFAULT_PAGE_SIZE {0x2000};
static constexpr Size MAXIMUM_PAGE_SIZE {0x10000};
static constexpr Size MINIMUM_WAL_LIMIT {0x20};
static constexpr Size DEFAULT_WAL_LIMIT {0x200};
static constexpr Size MAXIMUM_WAL_LIMIT {0x2000};
static constexpr Size MINIMUM_WAL_SPLIT {0};
static constexpr Size DEFAULT_WAL_SPLIT {25};
static constexpr Size MAXIMUM_WAL_SPLIT {50};

enum class LogLevel {
    TRACE,
    INFO,
    WARN,
    ERROR,
    OFF,
};

enum class LogTarget {
    FILE,
    STDERR,
    STDOUT,
    STDERR_COLOR,
    STDOUT_COLOR,
};

struct Options {
    Size page_size {DEFAULT_PAGE_SIZE};
    Size cache_size {DEFAULT_CACHE_SIZE};
    Size wal_limit {DEFAULT_WAL_LIMIT};
    Size wal_split {DEFAULT_WAL_SPLIT};
    Slice wal_prefix;
    LogLevel log_level {LogLevel::OFF};
    LogTarget log_target {};
    Storage *storage {};
};

} // namespace Calico

#endif // CALICO_OPTIONS_H
