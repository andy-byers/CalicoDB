#ifndef CALICO_OPTIONS_H
#define CALICO_OPTIONS_H

#include "slice.h"

namespace Calico {

class Storage;

static constexpr Size MINIMUM_PAGE_SIZE {0x100};
static constexpr Size DEFAULT_PAGE_SIZE {0x2000};
static constexpr Size MAXIMUM_PAGE_SIZE {0x10000};
static constexpr Size MINIMUM_LOG_MAX_SIZE {0xA000};
static constexpr Size DEFAULT_MAX_LOG_SIZE {0x100000};
static constexpr Size MAXIMUM_LOG_MAX_SIZE {0xA00000};
static constexpr Size MINIMUM_LOG_MAX_FILES {1};
static constexpr Size DEFAULT_MAX_LOG_FILES {4};
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
    STDOUT,
    STDERR,
    STDOUT_COLOR,
    STDERR_COLOR,
};

struct Options {
    Size page_size {DEFAULT_PAGE_SIZE};
    Size page_cache_size {};
    Size wal_buffer_size {};
    Slice wal_prefix;
    Size max_log_size {DEFAULT_MAX_LOG_SIZE};
    Size max_log_files {DEFAULT_MAX_LOG_FILES};
    LogLevel log_level {LogLevel::OFF};
    LogTarget log_target {};
    Storage *storage {};
};

} // namespace Calico

#endif // CALICO_OPTIONS_H