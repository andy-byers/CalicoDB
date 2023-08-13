// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "tx_impl.h"
#include "encoding.h"
#include "tree.h"

namespace calicodb
{

TxImpl::TxImpl(const Parameters &param)
    : m_schema(*param.pager, *param.status, *param.stat, param.scratch),
      m_errors(param.errors),
      m_status(param.status),
      m_pager(param.pager),
      m_writable(param.writable)
{
}

TxImpl::~TxImpl()
{
    m_schema.close();
    m_pager->finish();
    if (m_backref) {
        *m_backref = nullptr;
    }
}

#define ENSURE_WRITABLE                                              \
    do {                                                             \
        if (!m_writable) {                                           \
            return Status::not_supported("transaction is readonly"); \
        }                                                            \
    } while (0)

auto TxImpl::create_bucket(const BucketOptions &options, const Slice &name, Cursor **c_out) -> Status
{
    if (c_out) {
        *c_out = nullptr;
    }
    ENSURE_WRITABLE;
    auto s = *m_status;
    if (s.is_ok()) {
        s = m_schema.create_bucket(options, name, c_out);
    }
    return s;
}

auto TxImpl::open_bucket(const Slice &name, Cursor *&c_out) const -> Status
{
    c_out = nullptr;
    auto s = *m_status;
    if (s.is_ok()) {
        s = m_schema.open_bucket(name, c_out);
    }
    return s;
}

auto TxImpl::drop_bucket(const Slice &name) -> Status
{
    ENSURE_WRITABLE;
    auto s = *m_status;
    if (s.is_ok()) {
        s = m_schema.drop_bucket(name);
    }
    return s;
}

auto TxImpl::commit() -> Status
{
    ENSURE_WRITABLE;
    auto s = *m_status;
    if (s.is_ok()) {
        s = m_pager->commit();
    }
    return s;
}

auto TxImpl::vacuum() -> Status
{
    ENSURE_WRITABLE;
    auto s = *m_status;
    if (s.is_ok()) {
        s = m_schema.vacuum();
    }
    return s;
}

auto TxImpl::get(Cursor &c, const Slice &key, std::string *value) const -> Status
{
    auto s = *m_status;
    if (s.is_ok()) {
        const auto [tree, c_impl] = m_schema.unpack_and_use(c);
        s = tree.get(c_impl, key, value);
    }
    return s;
}

auto TxImpl::put(Cursor &c, const Slice &key, const Slice &value) -> Status
{
    ENSURE_WRITABLE;
    auto s = *m_status;
    if (s.is_ok()) {
        const auto [tree, c_impl] = m_schema.unpack_and_use(c);
        s = tree.put(c_impl, key, value);
    }
    return s;
}

auto TxImpl::erase(Cursor &c, const Slice &key) -> Status
{
    ENSURE_WRITABLE;
    auto s = *m_status;
    if (s.is_ok()) {
        const auto [tree, c_impl] = m_schema.unpack_and_use(c);
        s = tree.erase(c_impl, key);
    }
    return s;
}

auto TxImpl::erase(Cursor &c) -> Status
{
    ENSURE_WRITABLE;
    auto s = *m_status;
    if (s.is_ok()) {
        const auto [tree, c_impl] = m_schema.unpack_and_use(c);
        s = tree.erase(c_impl);
    }
    return s;
}

#undef ENSURE_WRITABLE

} // namespace calicodb