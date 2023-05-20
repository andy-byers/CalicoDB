// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "model.h"

namespace calicodb::tools
{

ModelDB::~ModelDB()
{
    if (m_owns_store) {
        delete m_store;
    }
}

auto ModelDB::new_txn(bool, Txn *&out) -> Status
{
    out = new ModelTxn(*m_store);
    return Status::ok();
}

ModelTxn::~ModelTxn() = default;

auto ModelTxn::new_table(const TableOptions &options, const Slice &name, Table *&out) -> Status
{
    auto itr = m_temp.find(name.to_string());
    if (itr == end(m_temp)) {
        if (!options.create_if_missing) {
            return Status::invalid_argument("table does not exist");
        }
        itr = m_temp.insert(itr, {name.to_string(), KVMap()});
    } else if (options.error_if_exists) {
        return Status::invalid_argument("table exists");
    }
    out = new ModelTable(itr->second);
    return Status::ok();
}

ModelTable::~ModelTable() = default;

auto ModelTable::new_cursor() const -> Cursor *
{
    return new ModelCursor(*m_map);
}

ModelCursor::~ModelCursor() = default;

} // namespace calicodb::tools