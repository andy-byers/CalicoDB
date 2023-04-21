// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "txn_impl.h"
#include "encoding.h"
#include "tree.h"

namespace calicodb
{

TxnImpl::TxnImpl(Pager &pager, DBState &state)
    : m_schema(pager),
      m_pager(&pager),
      m_state(&state)
{
}

TxnImpl::~TxnImpl()
{
    m_pager->finish();
}

auto TxnImpl::status() const -> Status
{
    return m_state->status;
}

auto TxnImpl::new_table(const TableOptions &options, const std::string &name, Table *&out) -> Status
{
    return m_schema.new_table(options, name, out);
}

auto TxnImpl::drop_table(const std::string &name) -> Status
{
    return m_schema.drop_table(name);
}

auto TxnImpl::vacuum() -> Status
{
    (void)m_pager;
    return Status::ok();
}

} // namespace calicodb