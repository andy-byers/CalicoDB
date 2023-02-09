#ifndef CALICO_UTILS_LOGGING_H
#define CALICO_UTILS_LOGGING_H

#include <sstream>
#include "calico/slice.h"
#include "calico/status.h"
#include "calico/storage.h"

namespace Calico {

auto append_number(std::string &out, Size value) -> void;
auto append_escaped_string(std::string &out, const Slice &value) -> void;
auto number_to_string(Size value) -> std::string;
auto escape_string(const Slice &value) -> std::string;

namespace Impl {

    inline auto logv(std::ostream &os) -> void
    {
        os << '\n';
    }

    template<class First, class ...Rest>
    auto logv(std::ostream &os, First &&first, Rest &&...rest) -> void
    {
        os << first;
        logv(os, std::forward<Rest>(rest)...);
    }

} // namespace Impl

template<class ...Param>
auto logv(Logger *log, Param &&...param) -> void
{
    if (log) {
        std::ostringstream oss;
        Impl::logv(oss, std::forward<Param>(param)...);
        (void)log->write(oss.str());
    }
}

} // namespace Calico

#endif // CALICO_UTILS_LOGGING_H