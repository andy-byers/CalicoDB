// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "txn_impl.h"
#include "encoding.h"
#include "tree.h"

namespace calicodb
{

TxnImpl::TxnImpl(Pager &pager, Status &status, bool write)
    : m_schema_obj(pager, status),
      m_schema(m_schema_obj.new_cursor()),
      m_pager(&pager),
      m_status(&status),
      m_write(write)
{
}

TxnImpl::~TxnImpl()
{
    delete m_schema;
    m_pager->finish();
    if (m_backref) {
        *m_backref = nullptr;
    }
}

auto TxnImpl::status() const -> Status
{
    return *m_status;
}

auto TxnImpl::schema() const -> Cursor &
{
    return *m_schema;
}

auto TxnImpl::create_table(const TableOptions &options, const Slice &name, Table **tb_out) -> Status
{
    if (tb_out) {
        *tb_out = nullptr;
    }
    auto s = *m_status;
    if (s.is_ok()) {
        auto altered = options;
        if (altered.create_if_missing) {
            altered.create_if_missing = m_write;
        }
        s = m_schema_obj.create_table(altered, name, !m_write, tb_out);
    }
    return s;
}

auto TxnImpl::drop_table(const Slice &name) -> Status
{
    if (!m_write) {
        return Status::readonly();
    }
    auto s = *m_status;
    if (s.is_ok()) {
        // Schema class disallows dropping tables during readonly transactions.
        s = m_schema_obj.drop_table(name);
    }
    return s;
}

auto TxnImpl::commit() -> Status
{
    if (!m_write) {
        return Status::ok();
    }
    auto s = *m_status;
    if (s.is_ok()) {
        s = m_pager->commit();
    }
    return s;
}

auto TxnImpl::vacuum() -> Status
{
    if (!m_write) {
        return Status::readonly();
    }
    auto s = *m_status;
    if (s.is_ok()) {
        if (!(s = vacuum_freelist()).is_ok()) {
            m_pager->set_status(s);
        }
    }
    return s;
}

auto TxnImpl::vacuum_freelist() -> Status
{
    CALICODB_TRY(m_pager->refresh_state());
    Id pgid(m_pager->page_count());
    for (; Id::root() < pgid; --pgid.value) {
        bool success;
        CALICODB_TRY(m_schema_obj.vacuum_page(pgid, success));
        if (!success) {
            break;
        }
    }
    if (pgid.value == m_pager->page_count()) {
        // No pages available to vacuum: database is minimally sized.
        return Status::ok();
    }

    auto s = m_schema_obj.vacuum_finish();
    if (s.is_ok()) {
        m_pager->set_page_count(pgid.value);
    }
    return s;
}

auto TxnImpl::TEST_validate() const -> void
{
    m_schema_obj.TEST_validate();
}

} // namespace calicodb