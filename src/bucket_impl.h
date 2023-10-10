// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_BUCKET_IMPL_H
#define CALICODB_BUCKET_IMPL_H

#include "calicodb/bucket.h"
#include "cursor_impl.h"
#include "internal.h"
#include "tree.h"

namespace calicodb
{

class Pager;
struct Stats;

class BucketImpl : public Bucket
{
public:
    explicit BucketImpl(Pager &pager, const Status &status, Stats &stat);

    auto create_bucket(const Slice &name, Bucket **b_out) -> Status override;
    auto open_bucket(const Slice &name, Bucket *&b_out) const -> Status override;
    auto drop_bucket(const Slice &name) -> Status override;
    auto new_cursor() const -> Cursor * override;
    auto put(const Slice &key, const Slice &value) -> Status override;
    auto erase(const Slice &key) -> Status override;
    auto put(Cursor &c, const Slice &value) -> Status override;
    auto erase(Cursor &c) -> Status override;

    auto TEST_validate() const -> void;

private:
    friend class Tree;

    const Status *const m_status;
    Pager *const m_pager;
    char *const m_scratch;
    Stats *const m_stat;

    Tree m_map;
    CursorImpl m_internal;
    CursorImpl m_exposed;

    // List containing a tree for each open sub-bucket.
    mutable Tree::ListEntry m_trees = {};

    // Wrapper over m_exposed, returns a human-readable string for Cursor::value().
    Cursor *const m_schema;
};

} // namespace calicodb

#endif // CALICODB_BUCKET_IMPL_H
