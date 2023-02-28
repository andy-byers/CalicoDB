#include "calico/storage.h"
#include "posix_storage.h"

namespace calicodb
{

auto Storage::default_storage() -> Storage *
{
    return new PosixStorage;
}

} // namespace calicodb