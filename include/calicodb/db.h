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
class TxnHandler;

// On-disk collection of tables
class DB
{
public:
    // Open or create a CalicoDB database with the given filename
    // On success, stores a pointer to the heap-allocated database in `db` and returns OK. On
    // failure, sets `db` to nullptr and returns a non-OK status. The user is responsible for
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
    // If the property named "name" exists, returns true and stores the property value in "*out".
    // Otherwise, false is returned and "*out" is set to nullptr. The out parameter is optional:
    // if passed a nullptr, this method performs an existence check.
    virtual auto get_property(const Slice &name, std::string *out) const -> bool = 0;

    // Start a transaction
    // Stores a pointer to the heap-allocated transaction object in "txn" and returns OK on
    // success. Stores nullptr in "txn" and returns a non-OK status on failure.
    [[nodiscard]] virtual auto new_txn(bool write, Txn *&txn) -> Status = 0;

    // Convenience wrapper that runs a read-only transaction
    // Forwards the status returned by `handler`.
    [[nodiscard]] virtual auto view(TxnHandler &handler) -> Status;

    // Convenience wrapper that runs a read-write transaction
    // If `handler` returns an OK status, the transaction is committed. Otherwise, the
    // transaction is rolled back.
    [[nodiscard]] virtual auto update(TxnHandler &handler) -> Status;
};

class TxnHandler
{
public:
    explicit TxnHandler();
    virtual ~TxnHandler();

    [[nodiscard]] virtual auto exec(Txn &txn) -> Status = 0;
};

// Transaction on a CalicoDB database
// The lifetime of a transaction is the same as that of the Txn object representing it
// (see DB::start() and DB::finish()).
class Txn
{
public:
    explicit Txn();
    virtual ~Txn();

    Txn(Txn &) = delete;
    void operator=(Txn &) = delete;

    // Return the status associated with this transaction
    // On creation, a Txn will always have an OK status. Only read-write
    // transactions can have a non-OK status. The status may be set when a
    // non-const method fails on this object, or any Table created from it.
    // The rollback() method always clears the status (sets it to OK).
    [[nodiscard]] virtual auto status() const -> Status = 0;

    // Create or open a table on the database
    // Note that tables cannot be created during readonly transactions. A
    // non-OK status is returned in this case.
    [[nodiscard]] virtual auto new_table(const TableOptions &options, const std::string &name, Table *&out) -> Status = 0;

    // Remove a table from the database
    // REQUIRES: Transaction is writable
    [[nodiscard]] virtual auto drop_table(const std::string &name) -> Status = 0;

    // Defragment the database
    // REQUIRES: Transaction is writable
    [[nodiscard]] virtual auto vacuum() -> Status = 0;

    // Commit pending changes to the database
    // Returns an OK status if the commit operation was successful, and a non-OK status
    // on failure. Calling this method on a read-only transaction is a NOOP.
    [[nodiscard]] virtual auto commit() -> Status = 0;

    // Rollback pending changes to the database
    // Calling this method on a read-only transaction is a NOOP.
    virtual auto rollback() -> void = 0;
};

// Persistent, ordered mapping between keys and values
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
    // error if the record does not exist.
    [[nodiscard]] virtual auto erase(const Slice &key) -> Status = 0;
};

} // namespace calicodb

#endif // CALICODB_DB_H
