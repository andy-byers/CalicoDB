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
class Table;
class Txn;

// On-disk collection of tables
class DB
{
public:
    // Open or create a CalicoDB database with the given filename
    // On success, stores a pointer to the heap-allocated database in `*db` and returns OK. On
    // failure, sets `*db` to nullptr and returns a non-OK status. The user is responsible for
    // calling delete on the database handle when it is no longer needed.
    [[nodiscard]] static auto open(const Options &options, const std::string &filename, DB *&db) -> Status;

    // Delete the contents of the specified database from stable storage
    // Deletes every file associated with the database named `filename` and returns OK on
    // success. Returns a non-OK status on failure. `options` should hold the same options
    // that were used to create the database (`options` must at least specify the WAL and
    // info log paths, if non-default values were used).
    [[nodiscard]] static auto destroy(const Options &options, const std::string &filename) -> Status;

    explicit DB();
    virtual ~DB();

    // Prevent move/copy
    DB(DB &) = delete;
    void operator=(DB &) = delete;

    // Get a human-readable string describing a named database property
    // If the property named `name` exists, returns true and stores the property value in `*out`.
    // Otherwise, false is returned and `*out` is set to nullptr. The `out` parameter is optional:
    // if passed a nullptr, this method performs an existence check.
    virtual auto get_property(const Slice &name, std::string *out) const -> bool = 0;

    // Write modified pages from the write-ahead log (WAL) back to the database file
    // If `reset` is true, steps are taken to make sure that the next writer will reset the WAL
    // (start writing from the beginning of the file again). This includes blocking until all
    // other connections are finished using the WAL. Additional checkpoints are run (a) when
    // the database is closed, and (b) when a database is opened that has a WAL on disk. Note
    // that in the case of (b), `reset` is false.
    [[nodiscard]] virtual auto checkpoint(bool reset) -> Status = 0;

    // Run a read-only transaction
    // Forwards the Status returned by the callable `fn`.
    // REQUIRES: Status Fn::operator()(Txn &) is implemented.
    template <class Fn>
    [[nodiscard]] auto view(Fn &&fn) -> Status;

    // Run a read-write transaction
    // If the callable `fn` returns an OK status, the transaction is committed. Otherwise,
    // the transaction is rolled back.
    // REQUIRES: Status Fn::operator()(Txn &) is implemented.
    template <class Fn>
    [[nodiscard]] auto update(Fn &&fn) -> Status;

    // Start a transaction manually
    // Stores a pointer to the heap-allocated transaction object in `*out` and returns OK on
    // success. Stores nullptr in `*out` and returns a non-OK status on failure. If `write` is true,
    // then the transaction is a read-write transaction, otherwise it is a readonly transaction.
    // The caller is responsible for calling delete on the Txn pointer when it is no longer needed.
    // NOTE: Consider using the DB::view()/DB::update() APIs instead of managing transactions
    // manually.
    [[nodiscard]] virtual auto new_txn(bool write, Txn *&out) -> Status = 0;
};

// Transaction on a CalicoDB database
// The lifetime of a transaction is the same as that of the Txn object representing it
// (see `DB::new_txn()`).
class Txn
{
public:
    explicit Txn();
    virtual ~Txn();

    Txn(Txn &) = delete;
    void operator=(Txn &) = delete;

    // Return the status associated with this transaction
    // On creation, a Txn will always have an OK status. Only read-write transactions
    // can have a non-OK status. The status may be set when a non-const method fails
    // on this object, or any Table created from it.
    [[nodiscard]] virtual auto status() const -> Status = 0;

    // Return a reference to a cursor that iterates over the database schema
    // NOTE: The returned cursor must not be used after the Txn itself is delete'd. The
    // underlying storage for this object is freed at that point.
    // The database schema is a special table stores the name and location of every
    // other table in the database. Calling Cursor::key() on the returned cursor gives a
    // table name, and calling Cursor::value() gives a (non-readable) variable-length
    // integer. See cursor.h for additional requirements pertaining to cursor use.
    [[nodiscard]] virtual auto schema() const -> Cursor & = 0;

    // Create or open a table on the database
    // On success, stores a table handle in `tb_out` and returns an OK status. The table
    // named `name` can then be accessed through `tb_out` until the transaction is
    // finished (or until `name` is dropped through Txn::drop_table()). `tb_out` is owned
    // by this Txn and must not be delete'd. On failure, stores nullptr in `tb_out` and
    // returns a non-OK status.
    // NOTE: Tables cannot be created during readonly transactions. A status for which
    // Status::is_readonly() evaluates to true is returned in this case.
    [[nodiscard]] virtual auto create_table(const TableOptions &options, const Slice &name, Table *&tb_out) -> Status = 0;

    // Remove a table from the database
    // REQUIRES: Transaction is writable and table `name` is not open
    // If a table named `name` exists, this method drops it and returns an OK status. If
    // `name` does not exist, returns a status for which Status::is_not_found() is true.
    [[nodiscard]] virtual auto drop_table(const Slice &name) -> Status = 0;

    // Defragment the database
    // REQUIRES: Transaction is writable
    [[nodiscard]] virtual auto vacuum() -> Status = 0;

    // Commit pending changes to the database
    // Returns an OK status if the commit operation was successful, and a non-OK status
    // on failure. Calling this method on a read-only transaction is a NOOP. If this
    // method is not called before the Txn object is destroyed, all pending changes will
    // be dropped. This method can be called more than once for a given Txn: file locks
    // are held until the Txn handle is delete'd.
    [[nodiscard]] virtual auto commit() -> Status = 0;
};

// Persistent, ordered mapping between keys and values
// NOTE: All Table objects created from a Txn must be closed (with operator delete())
// before the transaction is finished (see Txn::commit()).
class Table
{
public:
    explicit Table();
    virtual ~Table();

    Table(Table &) = delete;
    void operator=(Table &) = delete;

    // Return a heap-allocated cursor over the contents of the table
    // The cursor should be destroyed (using operator delete()) when it
    // is no longer needed.
    [[nodiscard]] virtual auto new_cursor() const -> Cursor * = 0;

    // Get the value associated with the given key
    // If the record with key `key` exists, assigns to `*value` the value
    // associated with it and returns an OK status. If the key does not
    // exist, sets `*value` to nullptr and returns a "not found" status.
    // If an error is encountered, returns a non-OK status as appropriate.
    [[nodiscard]] virtual auto get(const Slice &key, std::string *value) const -> Status = 0;

    // Create a mapping between `key` and `value` in the table
    // If a record with key `key` already exists, sets its value to `value`.
    // Otherwise, a new record is created. Returns an OK status on success,
    // and a non-OK status on failure.
    [[nodiscard]] virtual auto put(const Slice &key, const Slice &value) -> Status = 0;

    // Erase a record from the table
    // Returns a non-OK status if an error was encountered. It is not an
    // error if `key` does not exist.
    [[nodiscard]] virtual auto erase(const Slice &key) -> Status = 0;
};

class BusyHandler
{
public:
    explicit BusyHandler();
    virtual ~BusyHandler();

    virtual auto exec(unsigned attempts) -> bool = 0;
};

template <class Fn>
auto DB::view(Fn &&fn) -> Status
{
    Txn *txn;
    auto s = new_txn(false, txn);
    if (s.is_ok()) {
        s = fn(*txn);
        delete txn;
    }
    return s;
}

template <class Fn>
auto DB::update(Fn &&fn) -> Status
{
    Txn *txn;
    auto s = new_txn(true, txn);
    if (s.is_ok()) {
        s = fn(*txn);
        if (s.is_ok()) {
            s = txn->commit();
        }
        // Implicit rollback of all uncommitted changes.
        delete txn;
    }
    return s;
}

} // namespace calicodb

#endif // CALICODB_DB_H
