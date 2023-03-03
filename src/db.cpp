
#include "calicodb/calicodb.h"
#include "db_impl.h"
#include "utils.h"

namespace calicodb
{

auto DB::open(const Options &options, const Slice &filename, DB **db) -> Status
{
    auto *ptr = new DBImpl;
    auto s = ptr->open(options, filename);
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
