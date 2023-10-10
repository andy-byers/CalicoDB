// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_TX_H
#define CALICODB_TX_H

#include "db.h"

namespace calicodb
{

// calicodb/bucket.h
class Bucket;

// calicodb/cursor.h
class Cursor;

// Transaction on a CalicoDB database
// The lifetime of a transaction is the same as that of the Tx object representing it (see
// DB::new_tx()).
class Tx
{
public:
    explicit Tx();
    virtual ~Tx();

    Tx(Tx &) = delete;
    void operator=(Tx &) = delete;

    // Return the status associated with this transaction
    // On creation, a Tx will always have an OK status. Only read-write transactions
    // can have a non-OK status. The status is set when a routine on this object fails
    // such that the consistency of the underlying data store becomes questionable, or
    // corruption is detected in one of the files.
    virtual auto status() const -> Status = 0;

    // Return a reference to a cursor that iterates over the database schema
    // The database schema is a special bucket that stores the name and location of every
    // other bucket in the database. Calling Cursor::key() on a valid schema cursor gives a
    // bucket name. Calling Cursor::value() gives a bucket descriptor: a human-readable
    // description of options that the bucket was created with. This cursor must not be
    // used after the Tx that created it has been destroyed.
    // SEE: cursor.h (for additional Cursor usage requirements)
    [[nodiscard]] virtual auto schema() const -> Cursor & = 0;

    // Create a new bucket
    // Returns an OK status on success and a non-OK status on failure. If c_out is nonnull,
    // opens a cursor over the bucket contents and stores it in `*c_out`. The bucket can be
    // accessed via this cursor until the transaction is finished, or until `name` is dropped
    // using Tx::drop_bucket(). Note that the bucket will not persist in the database unless
    // Tx::commit() is called after the bucket has been created but before the transaction
    // is finished.
    virtual auto create_bucket(const BucketOptions &options, const Slice &name, Bucket **b_out) -> Status = 0;

    // Open an existing bucket
    // On success, stores a cursor over the bucket contents in `c_out` and returns an OK
    // status. If the bucket named `name` does not already exist, a status for which
    // Status::is_invalid_argument() evaluates to true is returned.
    virtual auto open_bucket(const Slice &name, Bucket *&b_out) const -> Status = 0;

    // Remove a bucket from the database
    // If a bucket named `name` exists, this method will attempt to remove it. If `name`
    // does not exist, returns a status for which Status::is_invalid_argument() evaluates
    // to true.
    virtual auto drop_bucket(const Slice &name) -> Status = 0;

    // Defragment the database
    // This routine reclaims all unused pages in the database. The database file will be
    // truncated the next time a checkpoint is run.
    virtual auto vacuum() -> Status = 0;

    // Commit pending changes to the database
    // Returns an OK status if the commit operation was successful, and a non-OK status
    // on failure. If this method is not called before the Tx object is destroyed, all
    // pending changes will be dropped. This method can be called more than once for a
    // given Tx: file locks are held until the Tx handle is delete'd.
    virtual auto commit() -> Status = 0;
};

template <class Fn>
auto DB::view(Fn &&fn) const -> Status
{
    Tx *tx;
    auto s = new_reader(tx);
    if (s.is_ok()) {
        const auto *immutable = tx;
        s = fn(*immutable);
        delete tx;
    }
    return s;
}

template <class Fn>
auto DB::update(Fn &&fn) -> Status
{
    Tx *tx;
    auto s = new_writer(tx);
    if (s.is_ok()) {
        s = fn(*tx);
        if (s.is_ok()) {
            s = tx->commit();
        }
        // Rollback all uncommitted changes.
        delete tx;
    }
    return s;
}

} // namespace calicodb

#endif // CALICODB_TX_H
