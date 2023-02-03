#ifndef CALICO_STORAGE_HELPERS_H
#define CALICO_STORAGE_HELPERS_H

#include "calico/storage.h"
#include "utils/utils.h"

namespace Calico {

template<class Reader>
[[nodiscard]]
auto read_exact(Reader &reader, Span out, Size offset) -> Status
{
    auto requested = out.size();
    auto s = reader.read(out.data(), requested, offset);

    if (s.is_ok() && out.size() != requested)
        return system_error("could not read exact: read {}/{} bytes", out.size(), requested);

    return s;
}

} // namespace Calico

#endif // CALICO_STORAGE_HELPERS_H
