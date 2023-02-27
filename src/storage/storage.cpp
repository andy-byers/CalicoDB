#include "calico/storage.h"
#include "posix_storage.h"

namespace Calico {

auto Storage::default_storage() -> Storage *
{
    return new PosixStorage;
}

} // namespace Calico