// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for contributor names.

#ifndef CALICODB_OPTIONS_H
#define CALICODB_OPTIONS_H

#include <string>

namespace calicodb
{

class Env;
class InfoLogger;

struct Options {
    // Size of a database page in bytes. This is the basic unit of I/O for the
    // database file. Data is read/written in page-sized chunks.
    std::size_t page_size {0x2000};

    // Size of the page cache in bytes.
    std::size_t cache_size {4'194'304}; // 4 MB

    // Alternate prefix to use for WAL segment files. Defaults to "dbname-wal-",
    // where "dbname" is the name of the database.
    std::string wal_prefix;

    // Custom destination for info log messages. Defaults to writing to a file
    // called "dbname-log", where "dbname" is the name of the database.
    InfoLogger *info_log {};

    // Custom storage environment.
    Env *env {};

    // If true, create the database if it is missing.
    bool create_if_missing {true};

    // If true, return with an error if the database already exists.
    bool error_if_exists {};
};

enum class AccessMode {
    kReadOnly,
    kReadWrite,
};

struct TableOptions {
    // If set to kReadOnly, calls to put() or erase() on the table will return with
    // an error.
    AccessMode mode {AccessMode::kReadWrite};
};

} // namespace calicodb

#endif // CALICODB_OPTIONS_H
