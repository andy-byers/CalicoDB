// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "tx_impl.h"
#include "encoding.h"

namespace calicodb
{

TxImpl::TxImpl(const Parameters &param)
    : m_schema(*param.pager, *param.stat),
      m_main(m_schema, m_schema.main_tree()),
      m_toplevel(m_schema.main_tree())
{
}

TxImpl::~TxImpl()
{
    m_schema.close_trees();
    m_schema.pager().finish();
    if (m_backref) {
        *m_backref = nullptr;
    }
}

auto TxImpl::commit() -> Status
{
    auto &pager = m_schema.pager();
    return pager_write(pager, [&pager] {
        return pager.commit();
    });
}

auto TxImpl::vacuum() -> Status
{
    return pager_write(m_schema.pager(), [&schema = m_schema] {
        return schema.vacuum();
    });
}

} // namespace calicodb