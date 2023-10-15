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

class BucketImpl;
class Pager;
struct Stats;

struct BucketList {
    BucketImpl *b;
    BucketList *prev_entry;
    BucketList *next_entry;
};

class BucketImpl
    : public Bucket,
      public HeapObject
{
public:
    explicit BucketImpl(Schema &schema, Tree &tree);
    ~BucketImpl() override;

    auto create_bucket(const Slice &key, Bucket **b_out) -> Status override;
    auto create_bucket_if_missing(const Slice &name, Bucket **b_out) -> Status override;
    auto open_bucket(const Slice &key, Bucket *&b_out) const -> Status override;
    auto drop_bucket(const Slice &key) -> Status override;
    auto new_cursor() const -> Cursor * override;
    auto put(const Slice &key, const Slice &value) -> Status override;
    auto put(Cursor &c, const Slice &value) -> Status override;
    auto get(const Slice &key, CALICODB_STRING *value_out) const -> Status override;
    auto erase(const Slice &key) -> Status override;
    auto erase(Cursor &c) -> Status override;

    auto TEST_validate() const -> void;

private:
    auto create_bucket_impl(const Slice &key, bool error_if_exists, Bucket **b_out) -> Status;
    [[nodiscard]] auto open_bucket_impl(Id root_id, Bucket *&b_out) const -> int;

    friend class Schema;

    mutable CursorImpl m_cursor;
    Schema *const m_schema;
    Tree *const m_tree;
};

} // namespace calicodb

#endif // CALICODB_BUCKET_IMPL_H
