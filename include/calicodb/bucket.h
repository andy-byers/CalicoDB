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

class Bucket
{
public:
    explicit Bucket();
    virtual ~Bucket();

    Bucket(Bucket &) = delete;
    void operator=(Bucket &) = delete;

    // Return a cursor over the contents of this bucket
    virtual auto new_cursor() const -> Cursor * = 0;

    // Create a nested bucket associated with the given `key`
    virtual auto create_bucket(const Slice &key, Bucket **b_out) -> Status = 0;

    // Open the nested bucket associated with the given `key`
    virtual auto open_bucket(const Slice &key, Bucket *&b_out) const -> Status = 0;

    // Drop the nested bucket associated with the given `key`
    virtual auto drop_bucket(const Slice &key) -> Status = 0;

    // Create a mapping between the given `key` and the given `value`
    virtual auto put(const Slice &key, const Slice &value) -> Status = 0;

    // Assign the given `value` to the record pointed to by `c`
    virtual auto put(Cursor &c, const Slice &value) -> Status = 0;

    // Erase the record identified by `key`
    // This method cannot be used to remove a nested bucket. Use drop_bucket() instead.
    virtual auto erase(const Slice &key) -> Status = 0;

    // Erase the record pointed to by `c`
    virtual auto erase(Cursor &c) -> Status = 0;
};

} // namespace calicodb

#endif // CALICODB_BUCKET_H
