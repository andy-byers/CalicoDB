#ifndef CALICO_DATABASE_H
#define CALICO_DATABASE_H

#include <string>
#include "slice.h"

namespace Calico {

class Cursor;
class Logger;
class Status;
class Storage;

enum class LogLevel {
    TRACE,
    INFO,
    WARN,
    ERROR,
    OFF,
};

struct Options {
    Size page_size {0x2000};
    Size cache_size {};
    Slice wal_prefix;
    LogLevel log_level {LogLevel::OFF};
    Logger *info_log {};
    Storage *storage {};
};

class Database {
public:
    [[nodiscard]] static auto open(const Slice &path, const Options &options, Database **db) -> Status;
    [[nodiscard]] static auto repair(const Slice &path, const Options &options) -> Status;
    [[nodiscard]] static auto destroy(const Slice &path, const Options &options) -> Status;

    virtual ~Database() = default;
    [[nodiscard]] virtual auto get_property(const Slice &name, std::string &out) const -> bool = 0;
    [[nodiscard]] virtual auto new_cursor() const -> Cursor * = 0;
    [[nodiscard]] virtual auto status() const -> Status = 0;
    [[nodiscard]] virtual auto vacuum() -> Status = 0;
    [[nodiscard]] virtual auto commit() -> Status = 0;
    [[nodiscard]] virtual auto get(const Slice &key, std::string &value) const -> Status = 0;
    [[nodiscard]] virtual auto put(const Slice &key, const Slice &value) -> Status = 0;
    [[nodiscard]] virtual auto erase(const Slice &key) -> Status = 0;
};

} // namespace Calico

#endif // CALICO_DATABASE_H
