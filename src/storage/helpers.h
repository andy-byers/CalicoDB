#ifndef CALICO_STORAGE_HELPERS_H
#define CALICO_STORAGE_HELPERS_H

#include "calico/storage.h"
#include "utils/utils.h"

namespace Calico {

template<class Reader>
[[nodiscard]] auto read_exact_at(Reader &reader, Span out, Size offset) -> Status
{
    auto requested = out.size();
    auto s = reader.read(out.data(), requested, offset);

    if (s.is_ok() && out.size() != requested) {
        return Status::system_error("incomplete read");
    }
    return s;
}

template<class Reader>
[[nodiscard]] auto read_exact(Reader &reader, Span out) -> Status
{
    auto requested = out.size();
    auto s = reader.read(out.data(), requested);

    if (s.is_ok() && out.size() != requested) {
        return Status::system_error("incomplete read");
    }
    return s;
}

inline auto make_path_prefix(std::string path) -> std::string
{
    // TODO: Hack
    while (path.back() == '/') {
        path.erase(end(path) - 1);
    }
    return path + '/';
}

} // namespace Calico

#endif // CALICO_STORAGE_HELPERS_H
