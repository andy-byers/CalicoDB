#ifndef CALICODB_OPTIONS_H
#define CALICODB_OPTIONS_H

#include <string>

namespace calicodb {

class Env;
class InfoLogger;

struct Options {
    std::size_t page_size {0x2000};
    std::size_t cache_size {};
    std::string wal_prefix;
    InfoLogger *info_log {};
    Env *env {};
    bool create_if_missing {true};
    bool error_if_exists {};
};

enum class AccessMode {
    kReadOnly,
    kReadWrite,
};

struct TableOptions {
    AccessMode mode {AccessMode::kReadWrite};
};

} // namespace calicodb

#endif // CALICODB_OPTIONS_H
