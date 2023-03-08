
#include "calicodb/calicodb.h"
#include "env_posix.h"
#include "db_impl.h"
#include "utils.h"

namespace calicodb
{

auto DB::open(const Options &options, const Slice &filename, DB **db) -> Status
{
    if (filename.is_empty()) {
        return Status::invalid_argument("path is empty");
    }
    auto sanitized = options;
    if (sanitized.cache_size == 0) {
        sanitized.cache_size = options.page_size * 64;
    }

    auto clean_filename = filename.to_string();
    const auto [dir, base] = split_path(clean_filename);
    clean_filename = join_paths(dir, base);

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

auto DB::repair(const Options &options, const Slice &filename) -> Status
{
    return DBImpl::repair(options, filename.to_string());
}

auto DB::destroy(const Options &options, const Slice &filename) -> Status
{
    return DBImpl::destroy(options, filename.to_string());
}

} // namespace calicodb
