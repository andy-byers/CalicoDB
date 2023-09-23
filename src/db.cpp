// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/db.h"
#include "calicodb/env.h"
#include "db_impl.h"
#include "header.h"
#include "internal.h"
#include "logging.h"
#include "temp.h"

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
    clip_to_range(sanitized.page_size, kMinPageSize, kMaxPageSize);
    clip_to_range(sanitized.cache_size, kMinFrameCount * sanitized.page_size, kMaxCacheSize);

    auto s = FileHdr::check_page_size(sanitized.page_size);
    if (!s.is_ok()) {
        return s;
    }

    // Allocate storage for the database filename.
    String db_name;
    if (append_strings(db_name, Slice(filename))) {
        return Status::no_memory();
    }

    // Determine and allocate storage for the WAL filename.
    int rc;
    String wal_name;
    if (sanitized.wal_filename[0] == '\0') {
        rc = append_strings(wal_name, Slice(db_name), kDefaultWalSuffix);
    } else {
        rc = append_strings(wal_name, Slice(sanitized.wal_filename));
    }
    if (rc) {
        return Status::no_memory();
    }

    s = Status::no_memory();
    if (sanitized.temp_database) {
        if (sanitized.env) {
            log(sanitized.info_log,
                "warning: ignoring options.env object @ %p "
                "(custom Env must not be used with temp database)",
                sanitized.env);
        }
        if (sanitized.wal) {
            log(sanitized.info_log,
                "warning: ignoring options.wal object @ %p "
                "(custom Wal must not be used with temp database)",
                sanitized.wal);
            // Opened by the pager at the start of the first transaction.
            sanitized.wal = nullptr;
        }
        sanitized.env = new_temp_env(sanitized.page_size * 4);
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
        sanitized.env = &default_env();
    }

    impl = new (std::nothrow) DBImpl({
        options,
        sanitized,
        move(db_name),
        move(wal_name),
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
