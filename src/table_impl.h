#ifndef CALICODB_TABLE_IMPL_H
#define CALICODB_TABLE_IMPL_H

#include "calicodb/table.h"
#include "types.h"

namespace calicodb {

class BPlusTree;

class TableImpl: public Table
{
public:
    ~TableImpl() override;
    [[nodiscard]] virtual auto new_cursor() const -> Cursor * override;
    [[nodiscard]] virtual auto get(const Slice &key, std::string &value) const -> Status override;
    [[nodiscard]] virtual auto put(const Slice &key, const Slice &value) -> Status override;
    [[nodiscard]] virtual auto erase(const Slice &key) -> Status override;

private:
    BPlusTree *m_tree {};
    Id m_root;
};

} // namespace calicodb

#endif // CALICODB_TABLE_IMPL_H
