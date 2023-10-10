// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_BUCKET_H
#define CALICODB_BUCKET_H

#include "slice.h"
#include "status.h"

namespace calicodb
{

// calicodb/cursor.h
class Cursor;

// Transaction on a CalicoDB database
// The lifetime of a transaction is the same as that of the Tx object representing it (see
// DB::new_tx()).
class Bucket
{
public:
    explicit Bucket();
    virtual ~Bucket();

    Bucket(Bucket &) = delete;
    void operator=(Bucket &) = delete;

    virtual auto create_bucket(const Slice &name, Bucket **b_out) -> Status = 0;
    virtual auto open_bucket(const Slice &name, Bucket *&b_out) const -> Status = 0;
    virtual auto drop_bucket(const Slice &name) -> Status = 0;

    virtual auto new_cursor() const -> Cursor * = 0;
    virtual auto put(const Slice &key, const Slice &value) -> Status = 0;
    virtual auto erase(const Slice &key) -> Status = 0;

    virtual auto put(Cursor &c, const Slice &value) -> Status = 0;
    virtual auto erase(Cursor &c) -> Status = 0;
};

} // namespace calicodb

#endif // CALICODB_BUCKET_H
