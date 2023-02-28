
#include "calicodb/calicodb.h"
#include "calicodb/env.h"
#include "db_impl.h"
#include "utils.h"

namespace calicodb
{

auto DB::open(const Slice &path, const Options &options, DB **db) -> Status
{
    auto *ptr = new DBImpl;
    auto s = ptr->open(path, options);
    if (!s.is_ok()) {
        delete ptr;
        return s;
    }

    *db = ptr;
    return Status::ok();
}

auto DB::repair(const Slice &path, const Options &options) -> Status
{
    return DBImpl::repair(path.to_string(), options);
}

auto DB::destroy(const Slice &path, const Options &options) -> Status
{
    return DBImpl::destroy(path.to_string(), options);
}

} // namespace calicodb
