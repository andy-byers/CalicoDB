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

// TODO: Get this from the build system!
static constexpr auto VERSION_NAME = "0.0.1";

} // namespace calico

#endif // CALICO_COMMON_H
