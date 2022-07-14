#ifndef CCO_UTILS_CRC_H
#define CCO_UTILS_CRC_H

#include "calico/bytes.h"

namespace cco::utils {

inline auto crc_32(BytesView) noexcept -> std::uint32_t
{
    // TODO: I'm getting rid of the zlib dependency, so we'll need to roll our own CRC function,
    //       or depend on another library.
    return {};
}

} // calico::utils

#endif // CCO_UTILS_CRC_H
