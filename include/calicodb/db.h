// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_DB_H
#define CALICODB_DB_H

#include "status.h"
#include <vector>

namespace calicodb
{

class Cursor;
class Env;
class InfoLogger;
class Table;
struct TableOptions;

struct Options {
    // Size of a database page in bytes. This is the basic unit of I/O for the
    // database file. Data is read/written in page-sized chunks. Must be a power-
    // of-two between 512 and 32768, inclusive.
    std::size_t page_size {16'384}; // 16 KB

    // Size of the page cache in bytes. Must be at least 16 pages (see above).
    std::size_t cache_size {4'194'304}; // 4 MB

    // Alternate prefix to use for WAL segment files. Defaults to "dbname-wal-",
    // where "dbname" is the name of the database.
    std::string wal_prefix;

    // Custom destination for info log messages. Defaults to writing to a file
    // called "dbname-log", where "dbname" is the name of the database. See env.h
    // for details.
    InfoLogger *info_log {};

    // Custom storage environment. See env.h for details.
    Env *env {};

    // If true, create the database if it is missing.
    bool create_if_missing {true};

    // If true, return with an error if the database already exists.
    bool error_if_exists {};
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
    // The "out" parameter is optional.
    [[nodiscard]] virtual auto get_property(const Slice &name, std::string *out) const -> bool = 0;

    // Get a handle to the default table.
    //
    // The default table is always open, and its handle is managed by the DB.
    [[nodiscard]] virtual auto default_table() const -> Table * = 0;

    // Get a status object describing the error state.
    //
    // If this status is not OK, then a fatal error has occurred. The database
    // must then be closed, as it will refuse to perform any more work. On the
    // next startup, the database will attempt to recover using the WAL.
    [[nodiscard]] virtual auto status() const -> Status = 0;

    // Run a checkpoint operation, which updates the logical contents of the
    // database to include all changes made since the last checkpoint.
    //
    // This operation affects all tables that have pending updates, as well as creation
    // and dropping of tables. Synchronizes both the WAL and the database file with the
    // underlying filesystem, and ensures that the WAL contains the necessary information
    // to recover from a crash.
    [[nodiscard]] virtual auto checkpoint() -> Status = 0;

    // Perform defragmentation and shrink the database file.
    //
    // This operation can be run at any time, however, it is a NOOP if not enough
    // records have been erased to cause database pages to become empty.
    [[nodiscard]] virtual auto vacuum() -> Status = 0;

    // List the name of each table created by this database, excluding the default table.
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

} // namespace calicodb

#endif // CALICODB_DB_H
