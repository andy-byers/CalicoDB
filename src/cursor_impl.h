// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_CURSOR_IMPL_H
#define CALICODB_CURSOR_IMPL_H

#include "buffer.h"
#include "calicodb/cursor.h"
#include "tree.h"

namespace calicodb
{

class Schema;

class CursorImpl
    : public Cursor,
      public HeapObject
{
    TreeCursor m_c;

public:
    explicit CursorImpl(Tree &tree);
    ~CursorImpl() override;

    [[nodiscard]] auto is_valid() const -> bool override;
    [[nodiscard]] auto handle() -> void * override;
    auto key() const -> Slice override;
    auto value() const -> Slice override;
    auto status() const -> Status override;
    auto seek_first() -> void override;
    auto seek_last() -> void override;
    auto seek(const Slice &key) -> void override;
    auto find(const Slice &key) -> void override;
    auto next() -> void override;
    auto previous() -> void override;

    auto check_integrity() const -> Status
    {
        return m_c.m_tree->check_integrity();
    }

    auto TEST_tree_cursor() -> TreeCursor &;
    auto TEST_check_state() const -> void;
};

} // namespace calicodb

#endif // CALICODB_CURSOR_IMPL_H
