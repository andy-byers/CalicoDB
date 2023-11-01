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

    [[nodiscard]] auto handle() -> void * override;
    [[nodiscard]] auto is_valid() const -> bool override;
    [[nodiscard]] auto is_bucket() const -> bool override;
    [[nodiscard]] auto key() const -> Slice override;
    [[nodiscard]] auto value() const -> Slice override;
    auto status() const -> Status override;
    void seek_first() override;
    void seek_last() override;
    void seek(const Slice &key) override;
    void find(const Slice &key) override;
    void next() override;
    void previous() override;

    auto check_integrity() const -> Status
    {
        return m_c.tree().check_integrity();
    }

    void TEST_check_state() const;
};

} // namespace calicodb

#endif // CALICODB_CURSOR_IMPL_H
