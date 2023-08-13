// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/db.h"
#include "alloc.h"
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

auto DB::open(const Options &options, const char *filename, DB *&db) -> Status
{
    DBImpl *impl = nullptr;
    StringPtr scratch_storage;
    auto sanitized = options;
    clip_to_range(sanitized.cache_size, kMinFrameCount * kPageSize, kMaxCacheSize);

    const auto db_name_len = std::strlen(filename);
    StringPtr db_name_storage(
        Alloc::to_string(Slice(filename, db_name_len)),
        db_name_len);
    if (!db_name_storage.is_valid()) {
        return Status::no_memory();
    }

    StringPtr wal_name_storage;
    auto s = Status::no_memory();
    if (0 == std::strcmp(sanitized.wal_filename, "")) {
        const auto suffix_len = std::strlen(kDefaultWalSuffix);
        wal_name_storage.reset(Alloc::combine(
            Slice(filename, db_name_len),
            Slice(kDefaultWalSuffix, suffix_len)));
    } else {
        wal_name_storage.reset(Alloc::to_string(sanitized.wal_filename));
    }
    if (!wal_name_storage.is_valid()) {
        goto cleanup;
    }

    scratch_storage.reset(Alloc::alloc_string(kTreeBufferLen));
    if (!scratch_storage.is_valid()) {
        goto cleanup;
    }

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

    impl = new(std::nothrow) DBImpl({
        sanitized,
        std::move(db_name_storage),
        std::move(wal_name_storage),
        std::move(scratch_storage),
    });
    if (impl) {
        s = impl->open(sanitized);
    } else {
        s = Status::no_memory();
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
