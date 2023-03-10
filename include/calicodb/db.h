#ifndef CALICODB_DB_H
#define CALICODB_DB_H

#include <string>

namespace calicodb
{

class Cursor;
class Env;
class InfoLogger;
class Slice;
class Status;
class Table;
struct Options;
struct TableOptions;

class DB
{
public:
    [[nodiscard]] static auto open(const Options &options, const std::string &filename, DB **db) -> Status;
    [[nodiscard]] static auto repair(const Options &options, const std::string &filename) -> Status;
    [[nodiscard]] static auto destroy(const Options &options, const std::string &filename) -> Status;

    virtual ~DB() = default;
    [[nodiscard]] virtual auto get_property(const Slice &name, std::string *out) const -> bool = 0;
    [[nodiscard]] virtual auto create_table(const TableOptions &options, const std::string &name, Table **out) -> Status = 0;
    [[nodiscard]] virtual auto drop_table(const std::string &name) -> Status = 0;
    [[nodiscard]] virtual auto checkpoint() -> Status = 0;
    [[nodiscard]] virtual auto status() const -> Status = 0;
    [[nodiscard]] virtual auto vacuum() -> Status = 0;
};

} // namespace calicodb

#endif // CALICODB_DB_H
