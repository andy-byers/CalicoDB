// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_OPTIONS_H
#define CALICODB_OPTIONS_H

#include <string>

namespace calicodb
{

class BusyHandler;
class Env;
class Sink;

// Size of a database page in bytes.
static constexpr std::size_t kPageSize = 4'096;

struct Options final {
    // Size of the page cache in bytes. Must be at least 16 pages.
    std::size_t cache_size = 1'024 * kPageSize; // 4 MB

    // Alternate filename to use for the WAL. If empty, creates the WAL at
    // "dbname-wal", where "dbname" is the name of the database.
    std::string wal_filename;

    // Destination for info log messages.
    Sink *info_log = nullptr;

    // Custom storage environment. See env.h for details.
    Env *env = nullptr;

    // Action to take while waiting on a file lock.
    BusyHandler *busy = nullptr;

    // If true, create the database if it is missing.
    bool create_if_missing = true;

    // If true, return with an error if the database already exists.
    bool error_if_exists = false;

    // If true, sync the WAL file on every commit. Hurts performance quite a bit,
    // but provides extra durability.
    bool sync = false;
};

struct TableOptions {
    bool create_if_missing = true;
    bool error_if_exists = false;
};

class BusyHandler {
public:
    explicit BusyHandler();
    virtual ~BusyHandler();

    virtual auto exec(unsigned attempts) -> bool = 0;
};

} // namespace calicodb

#endif // CALICODB_OPTIONS_H
