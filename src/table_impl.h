#ifndef CALICODB_TABLE_IMPL_H
#define CALICODB_TABLE_IMPL_H

#include "calicodb/table.h"
#include "page.h"
#include "types.h"

namespace calicodb
{

class Cursor;
class DBImpl;
class Env;
class Tree;
class WriteAheadLog;
struct LogicalPageId;

struct TableState {
    LogicalPageId root_id;
    Tree *tree {};
    bool is_open {};
};

class TableImpl : public Table
{
public:
    ~TableImpl() override;
    explicit TableImpl(std::string name, DBImpl &db, TableState &state, DBState &batch_size);
    [[nodiscard]] auto new_cursor() const -> Cursor * override;
    [[nodiscard]] auto get(const Slice &key, std::string *value) const -> Status override;
    [[nodiscard]] auto put(const Slice &key, const Slice &value) -> Status override;
    [[nodiscard]] auto erase(const Slice &key) -> Status override;

private:
    [[nodiscard]] auto root_id() const -> LogicalPageId;

    std::string m_name;
    mutable DBState *m_db_state {};
    TableState *m_state {};
    DBImpl *m_db {};
};

} // namespace calicodb

#endif // CALICODB_TABLE_IMPL_H
