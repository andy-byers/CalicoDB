#ifndef CALICO_COMMON_H
#define CALICO_COMMON_H

#include <cstdint>

#if (!defined(_WIN32) && !defined(_WIN64)) && (defined(__unix__) || defined(__unix) || defined(__APPLE__))
#  define CALICO_UNIX
#  ifdef __APPLE__
#    define CALICO_OSX
#  endif
#else
#  error "error: this platform is currently unsupported"
#endif // (!defined(_WIN32) && ...


namespace Calico {

// Common types.
using Byte = char;
using Size = std::uint64_t;

} // namespace Calico

#endif // CALICO_COMMON_H
