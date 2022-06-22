#ifndef CALICO_UTILS_CRC_H
#define CALICO_UTILS_CRC_H

#include "calico/bytes.h"
#include <zlib.h>

namespace calico {

inline auto crc_32(BytesView data) noexcept
{
    const auto ptr = reinterpret_cast<const Bytef*>(data.data());
    const auto len = static_cast<uInt>(data.size());
    return static_cast<uint32_t>(crc32(0, ptr, len));
}

} // calico

#endif // CALICO_UTILS_CRC_H
