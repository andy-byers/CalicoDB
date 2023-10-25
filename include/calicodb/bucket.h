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

// Sorted collection of key-value pairs in a database
// Buckets contain mappings from string keys to string values, as well as string keys
// to nested buckets. The Tx object in tx.h provides a reference to a single bucket,
// called the main bucket, which represents the entire database. All records and
// buckets are created inside the main bucket.
// Records and nested buckets are not compatible, that is, the methods provided for
// working with normal records (get, put, and erase) cannot be used to access or modify
// nested buckets. The *_bucket*() methods must be used to work with buckets. Accessing
// the wrong type of record will result in a status s for which s.is_incompatible_value()
// evaluates to true.
class Bucket
{
public:
    explicit Bucket();
    virtual ~Bucket();

    Bucket(Bucket &) = delete;
    void operator=(Bucket &) = delete;

    // Return a cursor over the contents of this bucket
    // The returned handle is owned by the caller and must be deleted when it is no
    // longer needed. Returns nullptr if the allocation fails.
    [[nodiscard]] virtual auto new_cursor() const -> Cursor * = 0;

    // Create a nested bucket associated with the given `key`
    // If a bucket with the given `key` already exists, a status is returned for which
    // Status::is_invalid_argument() evaluates to true. The bucket handle `b_out` is
    // optional: if omitted, a bucket is created but not opened.
    virtual auto create_bucket(const Slice &key, Bucket **b_out) -> Status = 0;

    // Create a nested bucket associated with the given `key`
    // It is not an error is the bucket already exists. The bucket handle `b_out` is
    // optional: if omitted, a bucket is created but not opened.
    virtual auto create_bucket_if_missing(const Slice &key, Bucket **b_out) -> Status = 0;

    // Open the nested bucket associated with the given `key`
    virtual auto open_bucket(const Slice &key, Bucket *&b_out) const -> Status = 0;

    // Drop the nested bucket associated with the given `key`
    // If the nested bucket named `key` is still open, i.e. there is a Bucket * handle
    // referencing it that hasn't been deleted, then the records in `key` can be accessed
    // through that handle until it is closed.
    virtual auto drop_bucket(const Slice &key) -> Status = 0;

    // Create a mapping between the given `key` and the given `value`
    virtual auto put(const Slice &key, const Slice &value) -> Status = 0;

    // Get the record value associated with the given `key`
    virtual auto get(const Slice &key, CALICODB_STRING *value_out) const -> Status = 0;

    // Erase the record identified by `key`
    // This method cannot be used to remove a nested bucket. Use drop_bucket() instead.
    virtual auto erase(const Slice &key) -> Status = 0;

    // Assign the given `value` to the record referenced by `c`
    virtual auto put(Cursor &c, const Slice &value) -> Status = 0;

    // Erase the record referenced by `c`
    virtual auto erase(Cursor &c) -> Status = 0;
};

} // namespace calicodb

#endif // CALICODB_BUCKET_H
