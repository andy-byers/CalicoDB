#ifndef CCO_COMMON_H
#define CCO_COMMON_H

#include <cstdint>
#include <cstdlib>
#include <string>

#if (!defined(_WIN32) && !defined(_WIN64)) && (defined(__unix__) || defined(__unix) || defined(__APPLE__))
#  define CCO_UNIX
#  ifdef __APPLE__
#    define CCO_OSX
#  endif
#else
#  error "Error: This platform is currently unsupported"
#endif

namespace cco {

using Byte = char;
using Size = std::uint64_t;
using Index = std::uint64_t;

static constexpr auto VERSION_NAME = "0.0.1";

/**
 * Representation of a database record.
 */
struct Record {
    auto operator<(const cco::Record&) const -> bool;  // TODO: Seems to be necessary for std::sort()...
    std::string key; ///< The key by which records are ordered.
    std::string value; ///< The record value.
};

} // cco

auto operator<(const cco::Record&, const cco::Record&) -> bool;
auto operator>(const cco::Record&, const cco::Record&) -> bool;
auto operator<=(const cco::Record&, const cco::Record&) -> bool;
auto operator>=(const cco::Record&, const cco::Record&) -> bool;
auto operator==(const cco::Record&, const cco::Record&) -> bool;
auto operator!=(const cco::Record&, const cco::Record&) -> bool;

#endif // CCO_COMMON_H
