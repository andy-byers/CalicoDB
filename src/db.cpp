// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/db.h"
#include "db_impl.h"
#include "logging.h"
#include "temp.h"
#include "utils.h"

namespace calicodb
{

namespace
{
template <class T, class V>
constexpr auto clip_to_range(T &t, V min, V max) -> void
{
    if (static_cast<V>(t) > max) {
        t = max;
    }
    if (static_cast<V>(t) < min) {
        t = min;
    }
}
} // namespace

auto DB::open(const Options &options, const char *filename, DB *&db) -> Status
{
    DBImpl *impl = nullptr;
    auto sanitized = options;
    clip_to_range(sanitized.cache_size, kMinFrameCount * kPageSize, kMaxCacheSize);

    // Allocate storage for the database filename.
    String db_name;
    auto filename_len = std::strlen(filename);
    if (append_strings(db_name, Slice(filename, filename_len))) {
        return Status::no_memory();
    }

    // Determine and allocate storage for the WAL filename.
    int rc;
    String wal_name;
    if (const auto wal_filename_len = std::strlen(sanitized.wal_filename)) {
        rc = append_strings(
            wal_name,
            Slice(sanitized.wal_filename, wal_filename_len));
    } else {
        rc = append_strings(
            wal_name,
            Slice(db_name),
            kDefaultWalSuffix);
    }
    if (rc) {
        return Status::no_memory();
    }

    // Allocate scratch memory for working with database pages.
    UniqueBuffer<char> scratch;
    if (scratch.realloc(kTreeBufferLen)) {
        return Status::no_memory();
    }

    auto s = Status::no_memory();
    if (sanitized.temp_database) {
        if (sanitized.env != nullptr) {
            log(sanitized.info_log,
                "warning: ignoring options.env object @ %p "
                "(custom Env must not be used with temp database)",
                sanitized.env);
        }
        sanitized.env = new_temp_env();
        if (sanitized.env == nullptr) {
            goto cleanup;
        }
        // Only the following combination of lock_mode and sync_mode is supported for an
        // in-memory database. The database can only be accessed though this DB object,
        // and there is no file on disk to synchronize with.
        sanitized.lock_mode = Options::kLockExclusive;
        sanitized.sync_mode = Options::kSyncOff;
    }
    if (sanitized.env == nullptr) {
        sanitized.env = &Env::default_env();
    }

    impl = new (std::nothrow) DBImpl({
        sanitized,
        move(db_name),
        move(wal_name),
        move(scratch),
    });
    if (impl) {
        s = impl->open(sanitized);
    }

cleanup:
    if (!s.is_ok() && impl) {
        delete impl;
        impl = nullptr;
    }
    db = impl;
    return s;
}

DB::DB() = default;

DB::~DB() = default;

BusyHandler::BusyHandler() = default;

BusyHandler::~BusyHandler() = default;

auto DB::destroy(const Options &options, const char *filename) -> Status
{
    return DBImpl::destroy(options, filename);
}

} // namespace calicodb
