// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "tx_impl.h"
#include "encoding.h"
#include "tree.h"

namespace calicodb
{

TxImpl::TxImpl(Pager &pager, const Status &status, Stat &stat, char *scratch, bool writable)
    : m_schema_obj(pager, status, stat, scratch),
      m_status(&status),
      m_schema(m_schema_obj.new_cursor()),
      m_pager(&pager),
      m_writable(writable)
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

#define ENSURE_WRITABLE \
    do { \
        if (!m_writable) { \
            return Status::not_supported("transaction is readonly"); \
        } \
    } while (0)

auto TxImpl::create_bucket(const BucketOptions &options, const Slice &name, Bucket *b_out) -> Status
{
    ENSURE_WRITABLE;
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
    ENSURE_WRITABLE;
    auto s = *m_status;
    if (s.is_ok()) {
        s = m_schema_obj.drop_bucket(name);
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
        s = m_schema_obj.vacuum();
    }
    return s;
}

auto TxImpl::new_cursor(const Bucket &b) const -> Cursor *
{
    m_schema_obj.use_bucket(b);
    return static_cast<Tree *>(b.state)->new_cursor();
}

auto TxImpl::get(const Bucket &b, const Slice &key, std::string *value) const -> Status
{
    auto s = *m_status;
    if (s.is_ok()) {
        m_schema_obj.use_bucket(b);
        s = static_cast<Tree *>(b.state)->get(key, value);
    }
    return s;
}

auto TxImpl::put(const Bucket &b, const Slice &key, const Slice &value) -> Status
{
    ENSURE_WRITABLE;
    auto s = *m_status;
    if (s.is_ok()) {
        m_schema_obj.use_bucket(b);
        s = static_cast<Tree *>(b.state)->put(key, value);
    }
    return s;
}

auto TxImpl::put(Cursor &c, const Slice &key, const Slice &value) -> Status
{
    ENSURE_WRITABLE;
    auto s = *m_status;
    if (s.is_ok()) {
        auto *tree = Tree::get_tree(c);
        m_schema_obj.use_bucket(Bucket{tree});
        s = tree->put(c, key, value);
    }
    return s;
}

auto TxImpl::erase(const Bucket &b, const Slice &key) -> Status
{
    ENSURE_WRITABLE;
    auto s = *m_status;
    if (s.is_ok()) {
        m_schema_obj.use_bucket(b);
        s = static_cast<Tree *>(b.state)->erase(key);
    }
    return s;
}

auto TxImpl::erase(Cursor &c) -> Status
{
    ENSURE_WRITABLE;
    auto s = *m_status;
    if (s.is_ok()) {
        auto *tree = Tree::get_tree(c);
        m_schema_obj.use_bucket(Bucket{tree});
        s = tree->erase(c);
    }
    return s;
}

#undef ENSURE_WRITABLE

} // namespace calicodb