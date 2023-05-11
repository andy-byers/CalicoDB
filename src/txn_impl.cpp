// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "txn_impl.h"
#include "encoding.h"
#include "tree.h"

namespace calicodb
{

TxnImpl::TxnImpl(Pager &pager, Status &status, bool write)
    : m_schema(pager, status, write),
      m_pager(&pager),
      m_status(&status),
      m_write(write)
{
}

TxnImpl::~TxnImpl()
{
    m_pager->finish();
    if (m_backref) {
        *m_backref = nullptr;
    }
}

auto TxnImpl::status() const -> Status
{
    return *m_status;
}

auto TxnImpl::new_table(const TableOptions &options, const std::string &name, Table *&out) -> Status
{
    out = nullptr;
    CALICODB_TRY(*m_status);
    return m_schema.new_table(options, name, out);
}

auto TxnImpl::drop_table(const std::string &name) -> Status
{
    CALICODB_TRY(*m_status);
    // Schema object disallows dropping tables during readonly transactions.
    return m_schema.drop_table(name);
}

auto TxnImpl::commit() -> Status
{
    if (!m_write) {
        return Status::ok();
    }
    CALICODB_TRY(*m_status);
    return m_pager->commit();
}

auto TxnImpl::rollback() -> void
{
    if (m_write) {
        m_pager->rollback();
        m_schema.inform_live_cursors();
    }
}

auto TxnImpl::vacuum() -> Status
{
    if (!m_write) {
        return Status::readonly();
    }
    CALICODB_TRY(*m_status);
    m_pager->set_status(vacuum_freelist());
    return *m_status;
}

auto TxnImpl::vacuum_freelist() -> Status
{
    CALICODB_TRY(m_pager->refresh_state());
    Id pgid(m_pager->page_count());
    for (; Id::root() < pgid; --pgid.value) {
        bool success;
        CALICODB_TRY(m_schema.vacuum_page(pgid, success));
        if (!success) {
            break;
        }
    }
    if (pgid.value == m_pager->page_count()) {
        // No pages available to vacuum: database is minimally sized.
        return Status::ok();
    }

    m_pager->set_page_count(pgid.value);
    return m_schema.vacuum_finish();
}

auto TxnImpl::TEST_validate() const -> void
{
    m_schema.TEST_validate();
}

} // namespace calicodb