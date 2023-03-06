#ifndef CALICODB_TABLE_IMPL_H
#define CALICODB_TABLE_IMPL_H

#include "calicodb/table.h"
#include "types.h"

namespace calicodb {

class Cursor;
class DBImpl;
class Env;
class Tree;
class WriteAheadLog;

class TableImpl : public Table
{
public:
    ~TableImpl();
    explicit TableImpl(DBImpl &db, TableState &state, Status &status);
    [[nodiscard]] auto new_cursor() const -> Cursor * override;
    [[nodiscard]] auto get(const Slice &key, std::string *value) const -> Status override;
    [[nodiscard]] auto put(const Slice &key, const Slice &value) -> Status override;
    [[nodiscard]] auto erase(const Slice &key) -> Status override;
    [[nodiscard]] auto commit() -> Status override;

private:
    DBImpl *m_db {};
    TableState *m_state {};
    mutable Status *m_status {};
    std::size_t m_batch_size {};
};

} // namespace calicodb

#endif // CALICODB_TABLE_IMPL_H
