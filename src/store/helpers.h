#ifndef CALICO_STORE_HELPERS_H
#define CALICO_STORE_HELPERS_H

#include "calico/store.h"
#include <spdlog/fmt/fmt.h>

namespace calico {

template<class Reader>
[[nodiscard]]
auto read_exact(Reader &reader, Bytes out, Size offset) -> Status
{
    static constexpr auto FMT = "could not read exact: read {}/{} bytes";
    const auto requested = out.size();
    auto s = reader.read(out, offset);

    if (s.is_ok() && out.size() != requested)
        return Status::system_error(fmt::format(FMT, out.size(), requested));

    return s;
}

} // namespace calico

#endif // CALICO_STORE_HELPERS_H
