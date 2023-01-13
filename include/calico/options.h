#ifndef CALICO_OPTIONS_H
#define CALICO_OPTIONS_H

#include "slice.h"

namespace Calico {

class Storage;

static constexpr Size MINIMUM_PAGE_SIZE {0x100};
static constexpr Size DEFAULT_PAGE_SIZE {0x2000};
static constexpr Size MAXIMUM_PAGE_SIZE {0x10000};
static constexpr Size MINIMUM_LOG_MAX_SIZE {0xA000};
static constexpr Size DEFAULT_LOG_MAX_SIZE {0x100000};
static constexpr Size MAXIMUM_LOG_MAX_SIZE {0xA00000};
static constexpr Size MINIMUM_LOG_MAX_FILES {1};
static constexpr Size DEFAULT_LOG_MAX_FILES {4};
static constexpr Size MAXIMUM_LOG_MAX_FILES {32};

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
    Size page_cache_size {};
    Size wal_buffer_size {};
    Size log_max_size {DEFAULT_LOG_MAX_SIZE};
    Size log_max_files {DEFAULT_LOG_MAX_FILES};
    LogLevel log_level {LogLevel::OFF};
    LogTarget log_target {};
    Slice wal_prefix;
    Storage *storage {};
};

} // namespace Calico

#endif // CALICO_OPTIONS_H
