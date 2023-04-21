// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "table_impl.h"
#include "tree.h"

namespace calicodb
{

TableImpl::TableImpl(Tree *&tree)
    : m_tree(&tree)
{
}

TableImpl::~TableImpl()
{
    delete *m_tree;
    *m_tree = nullptr;
}

auto TableImpl::new_cursor() const -> Cursor *
{
    return CursorInternal::make_cursor(**m_tree);
}

auto TableImpl::get(const Slice &key, std::string *value) const -> Status
{
    return (*m_tree)->get(key, value);
}

auto TableImpl::put(const Slice &key, const Slice &value) -> Status
{
    return (*m_tree)->put(key, value);
}

auto TableImpl::erase(const Slice &key) -> Status
{
    return (*m_tree)->erase(key);
}

} // namespace calicodb