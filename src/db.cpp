// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/db.h"
#include "db_impl.h"
#include "env_posix.h"
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
    const auto clean_filename = cleanup_path(filename);

    auto sanitized = options;
    clip_to_range(sanitized.cache_size, kMinFrameCount * kPageSize, kMaxCacheSize);
    if (sanitized.wal_filename.empty()) {
        sanitized.wal_filename = clean_filename + kDefaultWalSuffix;
    } else {
        sanitized.wal_filename = cleanup_path(sanitized.wal_filename);
    }
    if (sanitized.env == nullptr) {
        sanitized.env = Env::default_env();
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
