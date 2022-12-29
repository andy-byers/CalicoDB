#ifndef CALICO_UTILS_CRC_H
#define CALICO_UTILS_CRC_H

#include "calico/bytes.h"

namespace calico {

inline auto crc_32(BytesView data) noexcept -> std::uint32_t
{
    // TODO: I'm getting rid of the zlib dependency, so we'll need to roll our own CRC function,
    //       or depend on another library. For now, here's an Adler32 checksum copy-pasted from
    //       Wikipedia, with some stylistic changes...
    static constexpr std::uint32_t MOD_ADLER {65521};
    std::uint32_t a {1}, b {0};

    // Process each byte of the data in order
    for (Size i {}, n {data.size()}; i < n; ++i) {
        a = (a + static_cast<std::uint8_t>(data[i])) % MOD_ADLER;
        b = (b + a) % MOD_ADLER;
    }

    return (b << 16) | a;
}

} // namespace calico

#endif // CALICO_UTILS_CRC_H