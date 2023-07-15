// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/db.h"
#include "db_impl.h"
#include "env_posix.h"
#include "temp.h"
#include "utils.h"

namespace calicodb
{

template <class T, class V>
static constexpr auto clip_to_range(T &t, V min, V max) -> void
{
    if (static_cast<V>(t) > max) {
        t = max;
    }
    if (static_cast<V>(t) < min) {
        t = min;
    }
}

auto DB::open(const Options &options, const std::string &filename, DB *&db) -> Status
{
    auto clean_filename = cleanup_path(filename);

    auto sanitized = options;
    clip_to_range(sanitized.cache_size, kMinFrameCount * kPageSize, kMaxCacheSize);
    if (sanitized.wal_filename.empty()) {
        sanitized.wal_filename = clean_filename + kDefaultWalSuffix;
    } else {
        sanitized.wal_filename = cleanup_path(sanitized.wal_filename);
    }
    if (sanitized.temp_database) {
        if (sanitized.env != nullptr) {
            log(sanitized.info_log,
                "warning: ignoring options.env object @ %p "
                "(custom Env must not be used with temp database)",
                sanitized.env);
        }
        if (clean_filename.empty()) {
            clean_filename = "TempDB";
        }
        sanitized.env = new_temp_env();
        // Only the following combination of lock_mode and sync_mode is supported for an
        // in-memory database. The database can only be accessed though this DB object,
        // and there is no file on disk to synchronize with.
        sanitized.lock_mode = Options::kLockExclusive;
        sanitized.sync_mode = Options::kSyncOff;
    }
    if (sanitized.env == nullptr) {
        sanitized.env = &Env::default_env();
    }

    auto *impl = new DBImpl(options, sanitized, clean_filename);
    auto s = impl->open(sanitized);

    if (!s.is_ok()) {
        delete impl;
        impl = nullptr;
    }
    db = impl;
    return s;
}

DB::DB() = default;

DB::~DB() = default;

Tx::Tx() = default;

Tx::~Tx() = default;

BusyHandler::BusyHandler() = default;

BusyHandler::~BusyHandler() = default;

auto DB::destroy(const Options &options, const std::string &filename) -> Status
{
    return DBImpl::destroy(options, filename);
}

} // namespace calicodb
