// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/db.h"
#include "db_impl.h"
#include "env_posix.h"
#include "logging.h"
#include "utils.h"

namespace calicodb
{

template <class T, class V>
static auto clip_to_range(T &t, V min, V max) -> void
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
    clip_to_range(sanitized.page_size, kMinPageSize, kMaxPageSize);
    clip_to_range(sanitized.cache_size, {}, kMaxCacheSize);
    if (!is_power_of_two(sanitized.page_size)) {
        sanitized.page_size = Options {}.page_size;
    }
    if (sanitized.wal_filename.empty()) {
        sanitized.wal_filename = clean_filename + kDefaultWalSuffix;
    }
    if (sanitized.env == nullptr) {
        sanitized.env = Env::default_env();
    }
    if (sanitized.info_log == nullptr) {
        const auto log_filename = clean_filename + kDefaultLogSuffix;
        auto s = sanitized.env->new_log_file(log_filename, sanitized.info_log);
        if (!s.is_ok()) {
            sanitized.info_log = nullptr;
        }
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

auto DB::repair(const Options &options, const std::string &filename) -> Status
{
    return DBImpl::repair(options, filename);
}

auto DB::destroy(const Options &options, const std::string &filename) -> Status
{
    return DBImpl::destroy(options, filename);
}

auto DB::new_cursor() const -> Cursor *
{
    return new_cursor(*default_table());
}

auto DB::get(const Slice &key, std::string *value) const -> Status
{
    return get(*default_table(), key, value);
}

auto DB::put(const Slice &key, const Slice &value) -> Status
{
    return put(*default_table(), key, value);
}

auto DB::erase(const Slice &key) -> Status
{
    return erase(*default_table(), key);
}

} // namespace calicodb
