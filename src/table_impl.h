#ifndef CALICODB_TABLE_IMPL_H
#define CALICODB_TABLE_IMPL_H

#include "calicodb/table.h"
#include "page.h"
#include "types.h"

namespace calicodb {

class Cursor;
class DBImpl;
class Env;
class Tree;
class WriteAheadLog;
struct LogicalPageId;

struct TableState {
    LogicalPageId root_id {LogicalPageId::unknown()};
    Lsn checkpoint_lsn;
    Tree *tree {};
    bool is_open {};
};

class TableImpl : public Table
{
public:
    ~TableImpl() override;
    explicit TableImpl(DBImpl &db, TableState &state, Status &status, std::size_t &batch_size);
    [[nodiscard]] auto new_cursor() const -> Cursor * override;
    [[nodiscard]] auto get(const Slice &key, std::string *value) const -> Status override;
    [[nodiscard]] auto put(const Slice &key, const Slice &value) -> Status override;
    [[nodiscard]] auto erase(const Slice &key) -> Status override;

private:
    [[nodiscard]] auto root_id() const -> LogicalPageId;

    DBImpl *m_db {};
    TableState *m_state {};
    mutable Status *m_status {};
    std::size_t *m_batch_size {};
};

} // namespace calicodb

#endif // CALICODB_TABLE_IMPL_H
