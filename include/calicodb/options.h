// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_OPTIONS_H
#define CALICODB_OPTIONS_H

#include <cstddef>
#include <cstdint>

#ifndef CALICODB_DEFAULT_PAGE_SIZE
#define CALICODB_DEFAULT_PAGE_SIZE 4'096U
#endif // CALICODB_DEFAULT_PAGE_SIZE

namespace calicodb
{

// calicodb/env.h
class Env;
class File;

// calicodb/stat.h
struct Stats;

// calicodb/wal.h
class Wal;

// calicodb/options.h (below)
class BusyHandler;
class Logger;

// Options to control the behavior of a database connection (passed to DB::open()
// and DB::destroy())
struct Options final {
    // Size of a database page in bytes. Must be a power of 2 between 512 and
    // 32768, inclusive.
    size_t page_size = CALICODB_DEFAULT_PAGE_SIZE;

    // Size of the page cache in bytes.
    size_t cache_size = 1'024 * page_size;

    // Run a checkpoint when the WAL has reached this number of frames. If
    // set to 0, only the necessary checkpoints are run automatically. These
    // include (a) when the database is closed, and (b) when the database is
    // opened and recovery is needed.
    size_t auto_checkpoint = 1'000;

    // Alternate filename to use for the WAL. If empty, creates the WAL at
    // "dbname-wal", where "dbname" is the name of the database.
    const char *wal_filename = nullptr;

    // Destination for info log messages.
    Logger *info_log = nullptr;

    // Custom storage environment. See env.h for details.
    Env *env = nullptr;

    // Custom write-ahead log. See wal.h for details.
    Wal *wal = nullptr;

    // Action to take while waiting on a file lock.
    BusyHandler *busy = nullptr;

    // If true, create the database if it is missing.
    bool create_if_missing = false;

    // If true, return with an error if the database already exists.
    bool error_if_exists = false;

    // If true, create the database in RAM only and never write anything to disk. The
    // database will persist for the duration of the process that created it, and a
    // lock_mode of kLockExclusive is implied.
    // If this option is used, the implementation will ignore both the "env" and "wal"
    // fields.
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

// Options to control the behavior of a WAL connection (passed to Wal::open())
struct WalOptions {
    Env *env;
    File *db;
    Stats *stat;
};

class BusyHandler
{
public:
    explicit BusyHandler();
    virtual ~BusyHandler();

    virtual auto exec(unsigned attempts) -> bool = 0;
};

// Controls the behavior of the WAL checkpoint routine. Used by DB::checkpoint(),
// which calls Wal::checkpoint().
// kCheckpointPassive causes the WAL to write back as many pages as possible without
// interfering with other connections. Other checkpointers are blocked, but readers
// and a single writer are allowed to run concurrently. kCheckpointFull causes exclusion
// of both writers and checkpointers. This makes sure that no pages are written after
// the checkpoint starts. kCheckpointRestart is like kCheckpointFull, except that after
// finishing, the checkpointer will block until all other connections are done with the
// WAL. This ensures that future writes will start overwriting prior contents at the
// start of the log, rather than continuing to grow the file.
enum CheckpointMode {
    kCheckpointPassive,
    kCheckpointFull,
    kCheckpointRestart,
};

struct CheckpointInfo {
    size_t backfill;
    size_t wal_size;
};

} // namespace calicodb

#endif // CALICODB_OPTIONS_H
