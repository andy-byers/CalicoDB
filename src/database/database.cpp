
#include "database_impl.h"
#include "calico/calico.h"
#include "calico/storage.h"
#include "utils/utils.h"

namespace Calico {

auto Database::open(const Slice &path, const Options &options, Database **db) -> Status
{
    auto *ptr = new DatabaseImpl;
    auto s = ptr->open(path, options);
    if (!s.is_ok()) {
        delete ptr;
        return s;
    }

    *db = ptr;
    return Status::ok();
}

auto Database::repair(const Slice &path, const Options &options) -> Status
{
    return DatabaseImpl::repair(path.to_string(), options);
}

auto Database::destroy(const Slice &path, const Options &options) -> Status
{
    return DatabaseImpl::destroy(path.to_string(), options);
}

} // namespace Calico
