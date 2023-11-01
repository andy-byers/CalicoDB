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

auto get_full_filename(Env &env, const char *filename, String &result_out) -> Status
{
    if (!filename || !filename[0]) {
        return Status::ok(); // In-memory database
    }
    const auto n = env.max_filename() + 1;
    if (result_out.resize(n, '\0')) {
        return Status::no_memory();
    }
    auto *ptr = result_out.data();
    auto s = env.full_filename(filename, ptr, n);
    if (s.is_ok() && result_out.resize(std::strlen(ptr))) {
        s = Status::no_memory();
    }
    return s;
}

} // namespace

auto DB::open(const Options &options, const char *filename, DB *&db) -> Status
{
    db = nullptr;
    DBImpl *impl = nullptr;
    auto sanitized = options;
    clip_to_range(sanitized.page_size, kMinPageSize, kMaxPageSize);
    clip_to_range(sanitized.cache_size, kMinFrameCount * sanitized.page_size, kMaxCacheSize);

    auto s = FileHdr::check_page_size(sanitized.page_size);
    if (!s.is_ok()) {
        return s;
    }
    String db_name;
    String wal_name;
    UserPtr<Env> temp_env;
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
            // Created in DBImpl::open().
            sanitized.wal = nullptr;
        }
        // Keep the in-memory Env object in temp_env until the DBImpl can take ownership.
        temp_env.reset(new_temp_env(sanitized.page_size * 4));
        if (!temp_env) {
            goto cleanup;
        }
        sanitized.env = temp_env.get();
        // Only the following combination of lock_mode and sync_mode is supported for an
        // in-memory database. The database can only be accessed though this DB object,
        // and there is no file on disk to synchronize with.
        sanitized.lock_mode = Options::kLockExclusive;
        sanitized.sync_mode = Options::kSyncOff;
    }
    if (sanitized.env == nullptr) {
        sanitized.env = &default_env();
    }

    // Determine absolute paths for the database and WAL.
    s = get_full_filename(*sanitized.env, filename, db_name);
    if (s.is_ok()) {
        if (sanitized.wal_filename) {
            s = get_full_filename(*sanitized.env, sanitized.wal_filename, wal_name);
        } else if (append_strings(wal_name, Slice(db_name), kDefaultWalSuffix)) {
            s = Status::no_memory();
        }
    }
    if (!s.is_ok()) {
        return s;
    }

    impl = new (std::nothrow) DBImpl({
        options,
        sanitized,
        move(db_name),
        move(wal_name),
    });
    if (impl) {
        // DBImpl object now owns all memory allocated in this function.
        temp_env.release();
        s = impl->open(sanitized);
    } else {
        s = Status::no_memory();
    }

cleanup:
    if (!s.is_ok()) {
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
