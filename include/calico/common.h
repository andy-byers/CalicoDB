#ifndef CALICO_COMMON_H
#define CALICO_COMMON_H

#include <cstdint>
#include <cstdlib>
#include <string>

#if (!defined(_WIN32) && !defined(_WIN64)) && (defined(__unix__) || defined(__unix) || defined(__APPLE__))
#  define CALICO_UNIX
#  ifdef __APPLE__
#    define CALICO_OSX
#  endif
#else
#  error "Error: This platform is currently unsupported"
#endif

namespace calico {

using Byte = char;
using Size = std::uint64_t;

static constexpr auto VERSION_NAME = "0.0.1";

/**
 * Representation of a database record.
 */
struct Record {
    auto operator<(const calico::Record&) const -> bool;  // TODO: Seems to be necessary for std::sort()...
    std::string key; ///< The key by which records are ordered.
    std::string value; ///< The record value.
};

} // cco

auto operator<(const calico::Record&, const calico::Record&) -> bool;
auto operator>(const calico::Record&, const calico::Record&) -> bool;
auto operator<=(const calico::Record&, const calico::Record&) -> bool;
auto operator>=(const calico::Record&, const calico::Record&) -> bool;
auto operator==(const calico::Record&, const calico::Record&) -> bool;
auto operator!=(const calico::Record&, const calico::Record&) -> bool;

#endif // CALICO_COMMON_H
