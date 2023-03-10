
#include "calicodb/calicodb.h"
#include "db_impl.h"
#include "env_posix.h"
#include "utils.h"

namespace calicodb
{

auto DB::open(const Options &options, const std::string &filename, DB **db) -> Status
{
    if (filename.empty()) {
        return Status::invalid_argument("path is empty");
    }
    auto sanitized = options;
    if (sanitized.cache_size == 0) {
        sanitized.cache_size = options.page_size * 64;
    }

    const auto [dir, base] = split_path(filename);
    const auto clean_filename = join_paths(dir, base);

    if (sanitized.wal_prefix.empty()) {
        sanitized.wal_prefix = clean_filename + kDefaultWalSuffix;
    }
    if (sanitized.env == nullptr) {
        sanitized.env = Env::default_env();
    }
    if (sanitized.info_log == nullptr) {
        const auto log_filename = clean_filename + kDefaultLogSuffix;
        CDB_TRY(sanitized.env->new_info_logger(log_filename, &sanitized.info_log));
    }

    auto *ptr = new DBImpl {options, sanitized, clean_filename};
    auto s = ptr->open(sanitized);
    if (!s.is_ok()) {
        delete ptr;
        return s;
    }

    *db = ptr;
    return Status::ok();
}

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
    return new_cursor(default_table());
}

auto DB::get(const Slice &key, std::string *value) const -> Status
{
    return get(default_table(), key, value);
}

auto DB::put(const Slice &key, const Slice &value) -> Status
{
    return put(default_table(), key, value);
}

auto DB::erase(const Slice &key) -> Status
{
    return erase(default_table(), key);
}

} // namespace calicodb
