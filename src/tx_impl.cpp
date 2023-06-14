// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "tx_impl.h"
#include "encoding.h"
#include "tree.h"

namespace calicodb
{

TxImpl::TxImpl(Pager &pager, const Status &status, char *scratch)
    : m_schema_obj(pager, status, scratch),
      m_status(&status),
      m_schema(m_schema_obj.new_cursor()),
      m_pager(&pager)
{
}

TxImpl::~TxImpl()
{
    delete m_schema;
    m_schema_obj.close();
    m_pager->finish();
    if (m_backref) {
        *m_backref = nullptr;
    }
}

auto TxImpl::create_bucket(const BucketOptions &options, const Slice &name, Bucket *b_out) -> Status
{
    auto s = *m_status;
    if (s.is_ok()) {
        s = m_schema_obj.create_bucket(options, name, b_out);
    }
    return s;
}

auto TxImpl::open_bucket(const Slice &name, Bucket &b_out) const -> Status
{
    auto s = *m_status;
    if (s.is_ok()) {
        s = m_schema_obj.open_bucket(name, b_out);
    }
    return s;
}

auto TxImpl::drop_bucket(const Slice &name) -> Status
{
    auto s = *m_status;
    if (s.is_ok()) {
        s = m_schema_obj.drop_bucket(name);
    }
    return s;
}

auto TxImpl::commit() -> Status
{
    auto s = *m_status;
    if (s.is_ok()) {
        s = m_pager->commit();
    }
    return s;
}

auto TxImpl::vacuum() -> Status
{
    auto s = *m_status;
    if (s.is_ok()) {
        s = vacuum_freelist();
    }
    return s;
}

auto TxImpl::vacuum_freelist() -> Status
{
    return m_schema_obj.vacuum_freelist();
}

auto TxImpl::new_cursor(const Bucket &b) const -> Cursor *
{
    auto *cursor = new CursorImpl(*static_cast<Tree *>(b.state));
    if (!m_status->is_ok()) {
        cursor->clear(*m_status);
    }
    return cursor;
}

auto TxImpl::get(const Bucket &b, const Slice &key, std::string *value) const -> Status
{
    auto s = *m_status;
    if (s.is_ok()) {
        s = static_cast<Tree *>(b.state)->get(key, value);
    }
    return s;
}

auto TxImpl::put(const Bucket &b, const Slice &key, const Slice &value) -> Status
{
    auto s = *m_status;
    if (s.is_ok()) {
        s = static_cast<Tree *>(b.state)->put(key, value);
    }
    return s;
}

auto TxImpl::erase(const Bucket &b, const Slice &key) -> Status
{
    auto s = *m_status;
    if (s.is_ok()) {
        s = static_cast<Tree *>(b.state)->erase(key);
    }
    return s;
}

} // namespace calicodb