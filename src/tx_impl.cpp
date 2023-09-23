// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "tx_impl.h"
#include "encoding.h"

namespace calicodb
{

TxImpl::TxImpl(const Parameters &param)
    : m_schema(*param.pager, *param.status, *param.stat),
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

auto TxImpl::open_bucket(const Slice &name, Cursor *&c_out) const -> Status
{
    c_out = nullptr;
    auto s = *m_status;
    if (s.is_ok()) {
        s = m_schema.open_bucket(name, c_out);
    }
    return s;
}

auto TxImpl::create_bucket(const BucketOptions &options, const Slice &name, Cursor **c_out) -> Status
{
    if (c_out) {
        *c_out = nullptr;
    }
    return run_write_operation([&schema = m_schema, &options, &name, c_out] {
        return schema.create_bucket(options, name, c_out);
    });
}

auto TxImpl::drop_bucket(const Slice &name) -> Status
{
    return run_write_operation([&schema = m_schema, &name] {
        return schema.drop_bucket(name);
    });
}

auto TxImpl::commit() -> Status
{
    return run_write_operation([&pager = *m_pager] {
        return pager.commit();
    });
}

auto TxImpl::vacuum() -> Status
{
    return run_write_operation([&schema = m_schema] {
        return schema.vacuum();
    });
}

auto TxImpl::put(Cursor &c, const Slice &key, const Slice &value) -> Status
{
    return run_write_operation([&schema = m_schema, &c, &key, &value] {
        const auto [tree, c_impl] = schema.unpack_cursor(c);
        return tree->put(*c_impl, key, value);
    });
}

auto TxImpl::erase(Cursor &c, const Slice &key) -> Status
{
    return run_write_operation([&schema = m_schema, &c, &key] {
        const auto [tree, c_impl] = schema.unpack_cursor(c);
        return tree->erase(*c_impl, key);
    });
}

auto TxImpl::erase(Cursor &c) -> Status
{
    return run_write_operation([&schema = m_schema, &c] {
        const auto [tree, c_impl] = schema.unpack_cursor(c);
        return tree->erase(*c_impl);
    });
}

} // namespace calicodb