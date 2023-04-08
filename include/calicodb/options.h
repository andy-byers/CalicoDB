// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_OPTIONS_H
#define CALICODB_OPTIONS_H

#include <string>

namespace calicodb
{

class Env;
class LogFile;

enum class AccessMode {
    kReadOnly,
    kReadWrite,
};

struct Options {
    // Size of a database page in bytes. This is the basic unit of I/O for the
    // database file. Data is read/written in page-sized chunks. Must be a power-
    // of-two between 512 and 65536, inclusive.
    std::size_t page_size = 16'384; // 16 KB

    // Size of the page cache in bytes. Must be at least 16 pages (see above).
    std::size_t cache_size = 4'194'304; // 4 MB

    // Alternate filename to use for the WAL. If empty, creates the WAL at
    // "dbname-wal", where "dbname" is the name of the database.
    std::string wal_filename;

    // Custom destination for info log messages. Defaults to writing to a file
    // called "dbname-log", where "dbname" is the name of the database. See env.h
    // for details.
    LogFile *info_log = nullptr;

    // Custom storage environment. See env.h for details.
    Env *env = nullptr;

    // If true, create the database if it is missing.
    bool create_if_missing = true;

    // If true, return with an error if the database already exists.
    bool error_if_exists = false;

    // If true, sync the WAL file on every commit. Hurts performance quite a bit,
    // but provides extra durability.
    bool sync = false;
};

struct TableOptions {
    // If set to kReadOnly, calls to put() or erase() on the table will return with
    // an error.
    AccessMode mode = AccessMode::kReadWrite;
};

struct TxnOptions {

};

} // namespace calicodb

#endif // CALICODB_OPTIONS_H
