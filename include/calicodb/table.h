#ifndef CALICODB_TABLE_H
#define CALICODB_TABLE_H

#include <string>

namespace calicodb
{

class Cursor;
class DB;
class Slice;
class Status;

class Table
{
public:
    virtual ~Table() = default;
    [[nodiscard]] virtual auto new_cursor() const -> Cursor * = 0;
    [[nodiscard]] virtual auto get(const Slice &key, std::string &value) const -> Status = 0;
    [[nodiscard]] virtual auto put(const Slice &key, const Slice &value) -> Status = 0;
    [[nodiscard]] virtual auto erase(const Slice &key) -> Status = 0;
};

} // namespace calicodb

#endif // CALICODB_TABLE_H
