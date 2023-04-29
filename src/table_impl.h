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

    explicit TableImpl(Tree *&tree, bool write);
    ~TableImpl() override;
    [[nodiscard]] auto new_cursor() const -> Cursor * override;
    [[nodiscard]] auto get(const Slice &key, std::string *value) const -> Status override;
    [[nodiscard]] auto put(const Slice &key, const Slice &value) -> Status override;
    [[nodiscard]] auto erase(const Slice &key) -> Status override;

public:
    Tree **m_tree;
    bool m_readonly;
};

} // namespace calicodb

#endif // CALICODB_TABLE_IMPL_H
