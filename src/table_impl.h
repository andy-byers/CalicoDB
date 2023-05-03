// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_TABLE_IMPL_H
#define CALICODB_TABLE_IMPL_H

#include "calicodb/db.h"
#include "page.h"
#include <vector>

namespace calicodb
{

class Tree;

class TableImpl : public Table
{
public:
    friend class TxnImpl;

    explicit TableImpl(Tree *&tree, Status &status, bool write);
    ~TableImpl() override;
    [[nodiscard]] auto new_cursor() const -> Cursor * override;
    [[nodiscard]] auto get(const Slice &key, std::string *value) const -> Status override;
    [[nodiscard]] auto put(const Slice &key, const Slice &value) -> Status override;
    [[nodiscard]] auto erase(const Slice &key) -> Status override;

    [[nodiscard]] auto tree() -> Tree *
    {
        return *m_tree;
    }
    [[nodiscard]] auto tree() const -> const Tree *
    {
        return *m_tree;
    }

public:
    const Status *m_status;
    Tree **m_tree;
    bool m_readonly;
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
