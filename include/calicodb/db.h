#ifndef CALICODB_DB_H
#define CALICODB_DB_H

#include "slice.h"
#include <string>

namespace calicodb
{

class Cursor;
class InfoLogger;
class Status;
class Env;

struct Options {
    std::size_t page_size {0x2000};
    std::size_t cache_size {};
    Slice wal_prefix;
    InfoLogger *info_log {};
    Env *env {};
    bool create_if_missing {true};
    bool error_if_exists {};
};

class DB
{
public:
    [[nodiscard]] static auto open(const Options &options, const Slice &filename, DB **db) -> Status;
    [[nodiscard]] static auto repair(const Options &options, const Slice &filename) -> Status;
    [[nodiscard]] static auto destroy(const Options &options, const Slice &filename) -> Status;

    virtual ~DB() = default;
    [[nodiscard]] virtual auto get_property(const Slice &name, std::string &out) const -> bool = 0;
    [[nodiscard]] virtual auto new_cursor() const -> Cursor * = 0;
    [[nodiscard]] virtual auto status() const -> Status = 0;
    [[nodiscard]] virtual auto vacuum() -> Status = 0;
    [[nodiscard]] virtual auto commit() -> Status = 0;
    [[nodiscard]] virtual auto get(const Slice &key, std::string &value) const -> Status = 0;
    [[nodiscard]] virtual auto put(const Slice &key, const Slice &value) -> Status = 0;
    [[nodiscard]] virtual auto erase(const Slice &key) -> Status = 0;
};

} // namespace calicodb

#endif // CALICODB_DB_H
