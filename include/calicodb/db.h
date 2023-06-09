// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_DB_H
#define CALICODB_DB_H

#include "options.h"
#include "status.h"

namespace calicodb
{

class Cursor;
class Tx;

// Tag for starting a transaction that has writer capabilities
// SEE: DB::new_tx()
struct WriteTag {
};

// Opaque handle to an open bucket
// SEE: Tx::create_bucket(), Tx::open_bucket()
struct Bucket {
    void *state = nullptr;
};

// On-disk collection of buckets
class DB
{
public:
    // Open or create a CalicoDB database with the given filename
    // On success, stores a pointer to the heap-allocated database in `*db` and returns OK. On
    // failure, sets `*db` to nullptr and returns a non-OK status. The user is responsible for
    // calling delete on the database handle when it is no longer needed.
    static auto open(const Options &options, const std::string &filename, DB *&db_out) -> Status;

    // Delete the contents of the specified database from stable storage
    // Deletes every file associated with the database named `filename` and returns OK on
    // success. Returns a non-OK status on failure. `options` should hold the same options
    // that were used to create the database (`options` must at least specify the WAL and
    // info log paths, if non-default values were used).
    static auto destroy(const Options &options, const std::string &filename) -> Status;

    explicit DB();
    virtual ~DB();

    // Prevent move/copy
    DB(DB &) = delete;
    void operator=(DB &) = delete;

    // Get a human-readable string describing a named database property
    // If the property named `name` exists, returns true and stores the property value in
    // `*value_out`. Otherwise, false is returned and `value_out->clear()` is called. The
    // `value_out` parameter is optional: if passed a nullptr, this method performs an
    // existence check.
    virtual auto get_property(const Slice &name, std::string *value_out) const -> bool = 0;

    // Write modified pages from the write-ahead log (WAL) back to the database file
    // If `reset` is true, steps are taken to make sure that the next writer will reset the WAL
    // (start writing from the beginning of the file again). This includes blocking until all
    // other connections are finished using the WAL. Additional checkpoints are run (a) when
    // the database is closed, and (b) when a database is opened that has a WAL on disk. Note
    // that in the case of (b), `reset` is false.
    virtual auto checkpoint(bool reset) -> Status = 0;

    // Run a read-only transaction
    // REQUIRES: Status Fn::operator()(const Tx &) is implemented.
    // Forwards the Status returned by the callable `fn`. Note that the callable accepts a const
    // Tx reference, meaning methods that modify the database state cannot be called on it.
    template <class Fn>
    auto view(Fn &&fn) const -> Status;

    // Run a read-write transaction
    // REQUIRES: Status Fn::operator()(Tx &) is implemented.
    // If the callable `fn` returns an OK status, the transaction is committed. Otherwise,
    // the transaction is rolled back.
    template <class Fn>
    auto update(Fn &&fn) -> Status;

    // Start a transaction manually
    // Stores a pointer to the heap-allocated transaction object in `tx_out` and returns OK on
    // success. Stores nullptr in `tx_out` and returns a non-OK status on failure. If the WriteTag
    // overload is used, then the transaction is a read-write transaction, otherwise it is a
    // readonly transaction. The caller is responsible for calling delete on the Tx pointer when
    // it is no longer needed.
    // NOTE: Consider using the DB::view()/DB::update() API instead.
    virtual auto new_tx(const Tx *&tx_out) const -> Status = 0;
    virtual auto new_tx(WriteTag, Tx *&tx_out) -> Status = 0;
};

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
    // On success, stores a bucket handle in `*b_out` and returns an OK status. The bucket
    // named `name` can then be accessed with `*b_out` until the transaction is finished (or
    // until `name` is dropped through Tx::drop_bucket()). Returns a non-OK status on failure.
    // `b_out` is optional: if omitted, this method simply creates a bucket without handing
    // back a reference to it. Note that the bucket will not persist in the database unless
    // Tx::commit() is called after the bucket has been created.
    virtual auto create_bucket(const BucketOptions &options, const Slice &name, Bucket *b_out) -> Status = 0;

    // Open an existing bucket
    // Returns an OK status on success and a non-OK status on failure. If the bucket named
    // `name` does not exist already, a status for which Status::is_invalid_argument()
    // evaluates to true is returned.
    virtual auto open_bucket(const Slice &name, Bucket &b_out) const -> Status = 0;

    // Remove a bucket from the database
    // If a bucket named `name` exists, this method drops it and returns an OK status. If
    // `name` does not exist, returns a status for which Status::is_invalid_argument() is
    // true. If a bucket handle was obtained for `name` during this transaction, it must
    // not be used after this call succeeds.
    virtual auto drop_bucket(const Slice &name) -> Status = 0;

    // Defragment the database
    virtual auto vacuum() -> Status = 0;

    // Commit pending changes to the database
    // Returns an OK status if the commit operation was successful, and a non-OK status
    // on failure. Calling this method on a read-only transaction is a NOOP. If this
    // method is not called before the Tx object is destroyed, all pending changes will
    // be dropped. This method can be called more than once for a given Tx: file locks
    // are held until the Tx handle is delete'd.
    virtual auto commit() -> Status = 0;

    // Return a heap-allocated cursor over the contents of the bucket
    // The cursor should be destroyed (using operator delete()) when it
    // is no longer needed.
    [[nodiscard]] virtual auto new_cursor(const Bucket &b) const -> Cursor * = 0;

    // Get the value associated with the given key
    // If the record with key `key` exists, assigns to `*value` the value
    // associated with it and returns an OK status. If the key does not
    // exist, sets `*value` to nullptr and returns a "not found" status.
    // If an error is encountered, returns a non-OK status as appropriate.
    virtual auto get(const Bucket &b, const Slice &key, std::string *value) const -> Status = 0;

    // Create a mapping between `key` and `value`
    // If a record with key `key` already exists, sets its value to `value`.
    // Otherwise, a new record is created. Returns an OK status on success,
    // and a non-OK status on failure.
    virtual auto put(const Bucket &b, const Slice &key, const Slice &value) -> Status = 0;

    // Erase a record from the bucket
    // Returns a non-OK status if an error was encountered. It is not an
    // error if `key` does not exist.
    virtual auto erase(const Bucket &b, const Slice &key) -> Status = 0;
};

class BusyHandler
{
public:
    explicit BusyHandler();
    virtual ~BusyHandler();

    virtual auto exec(unsigned attempts) -> bool = 0;
};

template <class Fn>
auto DB::view(Fn &&fn) const -> Status
{
    const Tx *tx;
    auto s = new_tx(tx);
    if (s.is_ok()) {
        s = fn(*tx);
        delete tx;
    }
    return s;
}

template <class Fn>
auto DB::update(Fn &&fn) -> Status
{
    Tx *tx;
    auto s = new_tx(WriteTag(), tx);
    if (s.is_ok()) {
        s = fn(*tx);
        if (s.is_ok()) {
            s = tx->commit();
        }
        // Implicit rollback of all uncommitted changes.
        delete tx;
    }
    return s;
}

} // namespace calicodb

#endif // CALICODB_DB_H
