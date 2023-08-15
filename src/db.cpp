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
    auto sanitized = options;
    clip_to_range(sanitized.cache_size, kMinFrameCount * kPageSize, kMaxCacheSize);

    // Allocate storage for the database filename.
    auto filename_len = std::strlen(filename);
    auto *storage_ptr = Alloc::to_string(
        Slice(filename, filename_len));
    if (storage_ptr == nullptr) {
        return Status::no_memory();
    }
    UniqueBuffer db_name(storage_ptr, filename_len + 1);

    // Determine and allocate storage for the WAL filename.
    auto s = Status::no_memory();
    if (const auto wal_filename_len = std::strlen(sanitized.wal_filename)) {
        const Slice wal_filename(sanitized.wal_filename, wal_filename_len);
        storage_ptr = Alloc::to_string(wal_filename);
        filename_len = wal_filename_len;
    } else {
        const auto suffix_len = std::strlen(kDefaultWalSuffix);
        storage_ptr = Alloc::combine(
            Slice(filename, filename_len),
            Slice(kDefaultWalSuffix, suffix_len));
        filename_len = filename_len + suffix_len;
    }
    if (storage_ptr == nullptr) {
        return Status::no_memory();
    }
    UniqueBuffer wal_name(storage_ptr, filename_len + 1);

    // Allocate scratch memory for working with database pages.
    storage_ptr = static_cast<char *>(Alloc::alloc(kTreeBufferLen));
    if (storage_ptr == nullptr) {
        return Status::no_memory();
    }
    UniqueBuffer scratch(storage_ptr, kTreeBufferLen);

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
        std::move(db_name),
        std::move(wal_name),
        std::move(scratch),
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
