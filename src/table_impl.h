// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_TABLE_IMPL_H
#define CALICODB_TABLE_IMPL_H

#include "calicodb/db.h"
#include "page.h"
#include "tree.h"
#include <vector>

namespace calicodb
{

class TableImpl : public Table
{
public:
    friend class TxnImpl;

    explicit TableImpl(Pager &pager, Status &status, const Id *root, bool readonly);
    ~TableImpl() override;
    [[nodiscard]] auto new_cursor() const -> Cursor * override;
    [[nodiscard]] auto get(const Slice &key, std::string *value) const -> Status override;
    [[nodiscard]] auto put(const Slice &key, const Slice &value) -> Status override;
    [[nodiscard]] auto erase(const Slice &key) -> Status override;

    [[nodiscard]] auto TEST_tree() const -> const Tree &
    {
        return m_tree;
    }

private:
    const Status *m_status;
    mutable Tree m_tree;

    // NOTE: This field must be set by TxnImpl after the table object is allocated.
    bool m_readonly = true;
};

inline auto table_impl(Table *table) -> TableImpl *
{
    return reinterpret_cast<TableImpl *>(table);
}
inline auto table_impl(const Table *table) -> const TableImpl *
{
    return reinterpret_cast<const TableImpl *>(table);
}

} // namespace calicodb

#endif // CALICODB_TABLE_IMPL_H
