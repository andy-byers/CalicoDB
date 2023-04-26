// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_DB_H
#define CALICODB_DB_H

#include "options.h"
#include "status.h"
#include <vector>

namespace calicodb
{

class Cursor;
class Table;
class Txn;

// On-disk collection of tables
class DB
{
public:
    // Open or create a CalicoDB database with the given "filename"
    //
    // On success, store a pointer to the heap-allocated database in "db" and return OK. On
    // failure, set "db" to nullptr and return a non-OK status. The user is responsible for
    // calling delete on the database handle when it is no longer needed.
    [[nodiscard]] static auto open(const Options &options, const std::string &filename, DB *&db) -> Status;

    // Delete the contents of the specified database from stable storage
    //
    // Deletes every file associated with the database named "filename" and returns OK on
    // success. Returns a non-OK status on failure.
    [[nodiscard]] static auto destroy(const Options &options, const std::string &filename) -> Status;

    explicit DB();
    virtual ~DB();

    DB(const DB &) = delete;
    auto operator=(const DB &) -> void = delete;

    // Get a human-readable string describing a named property
    //
    // If the property named "name" exists, returns true and stores the property value in "*out".
    // Otherwise, false is returned and "*out" is set to nullptr. The out parameter is optional:
    // if passed a nullptr, this method performs an existence check.
    [[nodiscard]] virtual auto get_property(const Slice &name, std::string *out) const -> bool = 0;

    // Start a transaction
    //
    // Stores a pointer to the heap-allocated transaction object in "txn" and returns OK on
    // success. Stores nullptr in "txn" and returns a non-OK status on failure.
    [[nodiscard]] virtual auto start(bool write, Txn *&txn) -> Status = 0;

    // Finish a transaction
    //
    // Takes ownership of the transaction handle. A transaction handle obtained through
    // DB::start() must be passed to this method before (a) another transaction can be
    // started, or (b) the DB is closed.
    virtual auto finish(Txn *&txn) -> void = 0;
};

// Transaction on a CalicoDB database
//
// The lifetime of a transaction is the same as that of the Txn object representing it.
// The transaction starts when DB::start() is called to obtain a transaction handle, and
// finishes when the handle is passed back to DB::finish(). The methods Txn::commit() and
// Txn::rollback() can be called multiple times to perform multiple batches of updates
// while the Txn is live.
class Txn
{
public:
    virtual ~Txn();
    [[nodiscard]] virtual auto status() const -> Status = 0;
    [[nodiscard]] virtual auto new_table(const TableOptions &options, const std::string &name, Table *&out) -> Status = 0;
    [[nodiscard]] virtual auto drop_table(const std::string &name) -> Status = 0;
    [[nodiscard]] virtual auto vacuum() -> Status = 0;
    [[nodiscard]] virtual auto commit() -> Status = 0;
    virtual auto rollback() -> void = 0;
};

class Table
{
public:
    virtual ~Table();
    [[nodiscard]] virtual auto new_cursor() const -> Cursor * = 0;
    [[nodiscard]] virtual auto get(const Slice &key, std::string *value) const -> Status = 0;
    [[nodiscard]] virtual auto put(const Slice &key, const Slice &value) -> Status = 0;
    [[nodiscard]] virtual auto erase(const Slice &key) -> Status = 0;
};

} // namespace calicodb

#endif // CALICODB_DB_H
