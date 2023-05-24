// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "table_impl.h"
#include "tree.h"
#include "txn_impl.h"

namespace calicodb
{

TableImpl::TableImpl(Pager &pager, Status &status, const Id *root, bool readonly)
    : m_status(&status),
      m_tree(pager, root),
      m_readonly(readonly)
{
}

TableImpl::~TableImpl() = default;

auto TableImpl::new_cursor() const -> Cursor *
{
    auto *cursor = CursorInternal::make_cursor(m_tree);
    if (!m_status->is_ok()) {
        CursorInternal::invalidate(*cursor, *m_status);
    }
    return cursor;
}

auto TableImpl::get(const Slice &key, std::string *value) const -> Status
{
    auto s = *m_status;
    if (s.is_ok()) {
        s = m_tree.get(key, value);
    }
    return s;
}

auto TableImpl::put(const Slice &key, const Slice &value) -> Status
{
    if (m_readonly) {
        return Status::readonly();
    }
    auto s = *m_status;
    if (s.is_ok()) {
        s = m_tree.put(key, value);
    }
    return s;
}

auto TableImpl::erase(const Slice &key) -> Status
{
    if (m_readonly) {
        return Status::readonly();
    }
    auto s = *m_status;
    if (s.is_ok()) {
        s = m_tree.erase(key);
    }
    return s;
}

} // namespace calicodb