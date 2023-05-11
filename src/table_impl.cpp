// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "table_impl.h"
#include "tree.h"
#include "txn_impl.h"

namespace calicodb
{

TableImpl::TableImpl(Tree *&tree, Status &status, bool write)
    : m_status(&status),
      m_tree(&tree),
      m_readonly(!write)
{
}

TableImpl::~TableImpl()
{
    delete *m_tree;
    *m_tree = nullptr;
}

auto TableImpl::new_cursor() const -> Cursor *
{
    auto *cursor = CursorInternal::make_cursor(**m_tree);
    if (!m_status->is_ok()) {
        CursorInternal::invalidate(*cursor, *m_status);
    }
    return cursor;
}

auto TableImpl::get(const Slice &key, std::string *value) const -> Status
{
    CALICODB_TRY(*m_status);
    return (*m_tree)->get(key, value);
}

auto TableImpl::put(const Slice &key, const Slice &value) -> Status
{
    if (m_readonly) {
        return Status::readonly();
    }
    CALICODB_TRY(*m_status);
    return (*m_tree)->put(key, value);
}

auto TableImpl::erase(const Slice &key) -> Status
{
    if (m_readonly) {
        return Status::readonly();
    }
    CALICODB_TRY(*m_status);
    return (*m_tree)->erase(key);
}

} // namespace calicodb