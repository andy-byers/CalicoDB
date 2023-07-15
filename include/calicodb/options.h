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
class Logger;

// Size of a database page in bytes.
static constexpr std::size_t kPageSize = 4'096;

struct Options final {
    // Size of the page cache in bytes. Must be at least 16 pages.
    std::size_t cache_size = 1'024 * kPageSize; // 4 MB

    // Run a checkpoint when the WAL has reached this number of frames. If
    // set to 0, only the necessary checkpoints are run automatically. These
    // include (a) when the database is closed, and (b) when the database is
    // opened and recovery is needed.
    std::size_t auto_checkpoint = 1'000;

    // Alternate filename to use for the WAL. If empty, creates the WAL at
    // "dbname-wal", where "dbname" is the name of the database.
    std::string wal_filename;

    // Destination for info log messages.
    Logger *info_log = nullptr;

    // Custom storage environment. See env.h for details.
    Env *env = nullptr;

    // Action to take while waiting on a file lock.
    BusyHandler *busy = nullptr;

    // If true, create the database if it is missing.
    bool create_if_missing = true;

    // If true, return with an error if the database already exists.
    bool error_if_exists = false;

    // If true, create the database in RAM only and never write anything to disk. The
    // database will persist for the duration of the process that created it, and a
    // lock_mode of kLockExclusive is implied.
    bool temp_database = false;

    // Determines how often the operating system is asked to flush data to secondary
    // storage from the OS page cache.
    enum SyncMode {
        kSyncOff,    // No durability
        kSyncNormal, // Persist data on checkpoint
        kSyncFull,   // Persist data on commit
    } sync_mode = kSyncNormal;

    // Determines how much concurrency is allowed.
    enum LockMode {
        kLockNormal,    // Allow concurrent access
        kLockExclusive, // Exclude other connections
    } lock_mode = kLockNormal;
};

struct BucketOptions final {
    bool error_if_exists = false;
};

class BusyHandler
{
public:
    explicit BusyHandler();
    virtual ~BusyHandler();

    virtual auto exec(unsigned attempts) -> bool = 0;
};

} // namespace calicodb

#endif // CALICODB_OPTIONS_H
