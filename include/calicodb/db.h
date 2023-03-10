#ifndef CALICODB_DB_H
#define CALICODB_DB_H

#include <string>
#include <vector>

namespace calicodb
{

class Cursor;
class Env;
class InfoLogger;
class Slice;
class Status;
struct Options;
struct TableOptions;

class Table
{
public:
    virtual ~Table() = default;

    [[nodiscard]] virtual auto name() const -> const std::string & = 0;
};

class DB
{
public:
    static constexpr Table *kDefaultTable {};

    [[nodiscard]] static auto open(const Options &options, const std::string &filename, DB **db) -> Status;
    [[nodiscard]] static auto repair(const Options &options, const std::string &filename) -> Status;
    [[nodiscard]] static auto destroy(const Options &options, const std::string &filename) -> Status;

    virtual ~DB() = default;
    [[nodiscard]] virtual auto get_property(const Slice &name, std::string *out) const -> bool = 0;
    [[nodiscard]] virtual auto status() const -> Status = 0;
    [[nodiscard]] virtual auto checkpoint() -> Status = 0;
    [[nodiscard]] virtual auto vacuum() -> Status = 0;

    [[nodiscard]] virtual auto create_table(const TableOptions &options, const std::string &name, Table **out) -> Status = 0;
    [[nodiscard]] virtual auto list_tables(std::vector<std::string> *out) const -> Status = 0;
    [[nodiscard]] virtual auto drop_table(Table *table) -> Status = 0;
    virtual auto close_table(Table *table) -> void = 0;

    [[nodiscard]] virtual auto new_cursor(const Table *table) const -> Cursor * = 0;
    [[nodiscard]] virtual auto get(const Table *table, const Slice &key, std::string *value) const -> Status = 0;
    [[nodiscard]] virtual auto put(Table *table, const Slice &key, const Slice &value) -> Status = 0;
    [[nodiscard]] virtual auto erase(Table *table, const Slice &key) -> Status = 0;
};

} // namespace calicodb

#endif // CALICODB_DB_H
