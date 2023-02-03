#ifndef CALICO_DATABASE_H
#define CALICO_DATABASE_H

#include <string>
#include "slice.h"

namespace Calico {

struct Options;
class Cursor;
class Slice;
class Status;
class Storage;

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
    Size page_size {0x2000};
    Size page_cache_size {};
    Size wal_buffer_size {};
    Slice wal_prefix;
    Size max_log_size {0x100000};
    Size max_log_files {4};
    LogLevel log_level {LogLevel::OFF};
    LogTarget log_target {};
    Storage *storage {};
};

class Database {
public:
    virtual ~Database() = default;
    [[nodiscard]] static auto open(const Slice &path, const Options &options, Database **db) -> Status;
    [[nodiscard]] static auto destroy(const Slice &path, const Options &options) -> Status;

    [[nodiscard]] virtual auto get_property(const Slice &name) const -> std::string = 0;
    [[nodiscard]] virtual auto new_cursor() const -> Cursor * = 0;

    [[nodiscard]] virtual auto status() const -> Status = 0;
    [[nodiscard]] virtual auto commit() -> Status = 0;
    [[nodiscard]] virtual auto abort() -> Status = 0;

    [[nodiscard]] virtual auto get(const Slice &key, std::string &value) const -> Status = 0;
    [[nodiscard]] virtual auto put(const Slice &key, const Slice &value) -> Status = 0;
    [[nodiscard]] virtual auto erase(const Slice &key) -> Status = 0;
};

} // namespace Calico

#endif // CALICO_DATABASE_H
