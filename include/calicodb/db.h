// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

// TODO: New API! Really, this is the final change. It makes more sense this way, since read transactions are
//       required to get a consistent snapshot of the DB while reading, and write transactions have always
//       been necessary. Prevents us from having to manage transactions when the user doesn't start one.

//class DB
//{
//public:
//    [[nodiscard]] static auto open(const Options &options, const std::string &filename, DB *&db) -> Status;
//    [[nodiscard]] static auto destroy(const Options &options, const std::string &filename) -> Status;
//
//    explicit DB();
//    virtual ~DB();
//
//    // Prevent copies.
//    DB(const DB &) = delete;
//    auto operator=(const DB &) -> DB & = delete;
//
//    [[nodiscard]] virtual auto get_property(const Slice &name, std::string *out) const -> bool = 0;
//    [[nodiscard]] virtual auto begin(bool write, Txn *&txn) -> Status = 0;
//    [[nodiscard]] virtual auto commit(Txn *&txn) -> Status = 0;
//    virtual auto rollback(Txn *&txn) -> void = 0;
//};
//
//class Txn {
//public:
//    virtual ~Txn() = default;
//    [[nodiscard]] virtual auto status() const -> Status = 0;
//    [[nodiscard]] virtual auto open_table(const std::string &name) -> Table * = 0;
//    [[nodiscard]] virtual auto create_table(const std::string &name, Table *&out) -> Status = 0;
//    [[nodiscard]] virtual auto drop_table(Table *&table) -> Status = 0;
//    virtual auto close_table(Table *&table) -> void = 0;
//    [[nodiscard]] virtual auto vacuum() -> Status = 0;
//};
//
//class Table
//{
//public:
//    virtual ~Table();
//    [[nodiscard]] virtual auto new_cursor() const -> Cursor * = 0;
//    [[nodiscard]] virtual auto get(const Slice &key, std::string *value) const -> Status = 0;
//    [[nodiscard]] virtual auto put(const Slice &key, const Slice &value) -> Status = 0;
//    [[nodiscard]] virtual auto erase(const Slice &key) -> Status = 0;
//};


#ifndef CALICODB_DB_H
#define CALICODB_DB_H

#include "options.h"
#include "status.h"
#include <vector>

namespace calicodb
{

class Cursor;
class Env;
class Sink;
class Table;

struct TableOptions {
    // If set to kReadOnly, calls to put() or erase() on the table will return with
    // an error.
    AccessMode mode = AccessMode::kReadWrite;
};

struct TxnOptions {
};

// On-disk collection of tables.
class DB
{
public:
    // Open a database and obtain a heap-allocated object representing it.
    //
    // The caller is responsible for deleting the handle when it is no longer needed.
    // All objects allocated by the DB must be destroyed before the DB itself is
    // destroyed. On failure, "*db" is set to nullptr.
    [[nodiscard]] static auto open(const Options &options, const std::string &filename, DB *&db) -> Status;

    // Attempt to fix a database that cannot be opened due to corruption.
    //
    // TODO: Not implemented yet
    [[nodiscard]] static auto repair(const Options &options, const std::string &filename) -> Status;

    // Remove all files associated with a given database.
    [[nodiscard]] static auto destroy(const Options &options, const std::string &filename) -> Status;

    explicit DB();
    virtual ~DB();

    // Prevent copies.
    DB(const DB &) = delete;
    auto operator=(const DB &) -> void = delete;

    // Get a human-readable string describing the given property.
    //
    // The "out" parameter is optional: if omitted, this method performs an
    // existence check on the property named "name".
    [[nodiscard]] virtual auto get_property(const Slice &name, std::string *out) const -> bool = 0;

    // Get a handle to the default table.
    //
    // The default table is always open, and its handle is managed by the DB.
    [[nodiscard]] virtual auto default_table() const -> Table * = 0;

    // Get a status object describing the error state.
    //
    // If this status is not OK, then a fatal error has occurred. The database must then
    // be closed (or rolled back if in a user-initiated write transaction), as it will
    // refuse to perform any more work. On the next startup, the database will attempt
    // to recover using the WAL.
    [[nodiscard]] virtual auto status() const -> Status = 0;

    // Perform defragmentation.
    //
    // This operation can be run at any time, however, it is a NOOP if not enough records
    // have been erased to cause database pages to become empty.
    [[nodiscard]] virtual auto vacuum() -> Status = 0;

    // List the name of each user table created on this database.
    [[nodiscard]] virtual auto list_tables(std::vector<std::string> &out) const -> Status = 0;

    // Open a table and return an opaque handle to it.
    //
    // Table handles obtained through this method must be passed to either close_table()
    // or drop_table() before the database itself is closed. Note that the name "default"
    // is reserved for the default table, which is always open. If a table does not exist
    // named "name", one will be created.
    [[nodiscard]] virtual auto create_table(const TableOptions &options, const std::string &name, Table *&out) -> Status = 0;

    // Remove a table and its records from the database.
    //
    // This method destroys the table handle and sets "*table" to nullptr.
    [[nodiscard]] virtual auto drop_table(Table *&table) -> Status = 0;

    // Close a table.
    //
    // This method destroys the table handle and sets "*table" to nullptr.
    virtual auto close_table(Table *&table) -> void = 0;

    // Begin an explicit write transaction.
    //
    // If this method is not called, each modification is wrapped in its own write transaction.
    // It is the caller's responsibility to finish the transaction by calling either commit_txn()
    // or rollback_txn(). A pending transaction will be rolled back when the DB is closed. Returns
    // a unique integer representing the transaction. If a transaction has already been started,
    // that transaction's identifier will be returned.
    [[nodiscard]] virtual auto begin_txn(const TxnOptions &options) -> unsigned = 0;

    // Commit an explicit write transaction.
    //
    // Commits all modifications made during transaction "txn" to the database. This includes
    // creation and dropping of tables.
    [[nodiscard]] virtual auto commit_txn(unsigned txn) -> Status = 0;

    // Rollback an explicit write transaction.
    //
    // Rolls back all modifications for transaction "txn", including creation and dropping
    // of tables.
    [[nodiscard]] virtual auto rollback_txn(unsigned txn) -> Status = 0;

    // Get a heap-allocated cursor over the contents of "table".
    //
    // Cursors are invalid upon allocation and must have one of their seek*() methods called
    // before they can be used.
    [[nodiscard]] virtual auto new_cursor(const Table &table) const -> Cursor * = 0;

    // Get the value associated with the given key, if it exists.
    //
    // If the key does not exist, returns Status::not_found(). If "value" is nullptr, this
    // method is essentially an existence check for "key" in "table".
    [[nodiscard]] virtual auto get(const Table &table, const Slice &key, std::string *value) const -> Status = 0;

    // Set the entry in "table" associated with "key" to "value".
    [[nodiscard]] virtual auto put(Table &table, const Slice &key, const Slice &value) -> Status = 0;

    // Remove the record in "table" with the given key, if it exists.
    [[nodiscard]] virtual auto erase(Table &table, const Slice &key) -> Status = 0;

    // Overloads that direct access to the default table.
    [[nodiscard]] virtual auto new_cursor() const -> Cursor *;
    [[nodiscard]] virtual auto get(const Slice &key, std::string *value) const -> Status;
    [[nodiscard]] virtual auto put(const Slice &key, const Slice &value) -> Status;
    [[nodiscard]] virtual auto erase(const Slice &key) -> Status;
};

// Persistent ordered mapping from keys to values within a CalicoDB database.
//
// Keys are unique within each table.
class Table
{
public:
    virtual ~Table();

    // Get the name used to identify this table.
    [[nodiscard]] virtual auto name() const -> const std::string & = 0;
};

} // namespace calicodb

#endif // CALICODB_DB_H
