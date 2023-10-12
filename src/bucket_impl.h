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

class BucketImpl
    : public Bucket,
      public HeapObject
{
public:
    explicit BucketImpl(Schema &schema, Tree &tree);
    ~BucketImpl() override;

    auto create_bucket(const Slice &key, Bucket **b_out) -> Status override;
    auto open_bucket(const Slice &key, Bucket *&b_out) const -> Status override;
    auto drop_bucket(const Slice &key) -> Status override;
    auto new_cursor() const -> Cursor * override;
    auto put(const Slice &key, const Slice &value) -> Status override;
    auto erase(const Slice &key) -> Status override;
    auto put(Cursor &c, const Slice &value) -> Status override;
    auto erase(Cursor &c) -> Status override;

    auto TEST_validate() const -> void;

private:
    auto decode_and_check_root_id(const Slice &data, Id &root_id_out) const -> Status;

    friend class Tree;

    mutable CursorImpl m_cursor;
    Schema *const m_schema;
    Tree *const m_tree;
};

} // namespace calicodb

#endif // CALICODB_BUCKET_IMPL_H
