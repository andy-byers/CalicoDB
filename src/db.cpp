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

    // Allocate storage for the database filename. Note that if the filename is empty, a single byte
    // will be allocated to hold a '\0', so we won't attempt to allocate 0 bytes.
    auto filename_len = std::strlen(filename);
    auto db_name = UniqueBuffer::from_slice(
        Slice(filename, filename_len));
    if (db_name.is_empty()) {
        return Status::no_memory();
    }

    // Determine and allocate storage for the WAL filename.
    UniqueBuffer wal_name;
    if (const auto wal_filename_len = std::strlen(sanitized.wal_filename)) {
        wal_name = UniqueBuffer::from_slice(
            Slice(sanitized.wal_filename, wal_filename_len));
    } else {
        wal_name = UniqueBuffer::from_slice(
            Slice(filename, filename_len),
            Slice(kDefaultWalSuffix, std::strlen(kDefaultWalSuffix)));
    }
    if (wal_name.is_empty()) {
        return Status::no_memory();
    }

    // Allocate scratch memory for working with database pages.
    auto *scratch_ptr = static_cast<char *>(Alloc::malloc(kTreeBufferLen));
    if (scratch_ptr == nullptr) {
        return Status::no_memory();
    }
    UniqueBuffer scratch(scratch_ptr, kTreeBufferLen);

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
        std::move(db_name),
        std::move(wal_name),
        std::move(scratch),
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
