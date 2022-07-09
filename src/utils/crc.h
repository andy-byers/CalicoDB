#ifndef CALICO_UTILS_CRC_H
#define CALICO_UTILS_CRC_H

#include "calico/bytes.h"

namespace calico {

inline auto crc_32(BytesView) noexcept
{
    // TODO: I'm getting rid of the zlib dependency, so we'll need to roll our own CRC function,
    //       or depend on another library.
    return std::uint32_t {};
}

} // calico

#endif // CALICO_UTILS_CRC_H
