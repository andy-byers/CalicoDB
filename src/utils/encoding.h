/* 
 * encoding.h: Simple serialization/deserialization procedures based on those found in LevelDB.
 */
#ifndef CUB_UTILS_ENCODING_H
#define CUB_UTILS_ENCODING_H

#include "common.h"
#include "slice.h"

namespace cub {

inline auto get_uint16(const Byte *in) noexcept
{
    const auto src = reinterpret_cast<const uint8_t*>(in);
    return static_cast<uint16_t>(src[1] | (src[0] << 8));
}

inline auto get_uint16(RefBytes in) noexcept
{
    return get_uint16(in.data());
}

inline auto get_uint32(const Byte *in) noexcept
{
    const auto src = reinterpret_cast<const uint8_t *>(in);
    return static_cast<uint32_t>(src[3]) |
           (static_cast<uint32_t>(src[2]) << 8) |
           (static_cast<uint32_t>(src[1]) << 16) |
           (static_cast<uint32_t>(src[0]) << 24);
}

inline auto get_uint32(RefBytes in) noexcept
{
    return get_uint32(in.data());
}

inline auto get_uint64(const Byte *in) noexcept
{
    const auto src = reinterpret_cast<const uint8_t *>(in);
    return static_cast<uint64_t>(src[7]) |
           (static_cast<uint64_t>(src[6]) << 8) |
           (static_cast<uint64_t>(src[5]) << 16) |
           (static_cast<uint64_t>(src[4]) << 24) |
           (static_cast<uint64_t>(src[3]) << 32) |
           (static_cast<uint64_t>(src[2]) << 40) |
           (static_cast<uint64_t>(src[1]) << 48) |
           (static_cast<uint64_t>(src[0]) << 56);
}

inline auto get_uint64(RefBytes in) noexcept
{
    return get_uint64(in.data());
}

inline auto put_uint16(Byte *out, uint16_t value) noexcept
{
    auto *dst = reinterpret_cast<uint8_t *>(out);
    dst[1] = static_cast<uint8_t>(value);
    dst[0] = static_cast<uint8_t>(value >> 8);
}

inline auto put_uint16(MutBytes out, uint16_t value) noexcept
{
    put_uint16(out.data(), value);
}

inline auto put_uint32(Byte *out, uint32_t value) noexcept
{
    auto *dst = reinterpret_cast<uint8_t *>(out);
    dst[3] = static_cast<uint8_t>(value);
    dst[2] = static_cast<uint8_t>(value >> 8);
    dst[1] = static_cast<uint8_t>(value >> 16);
    dst[0] = static_cast<uint8_t>(value >> 24);
}

inline auto put_uint32(MutBytes out, uint32_t value) noexcept
{
    put_uint32(out.data(), value);
}

inline auto put_uint64(Byte *out, uint64_t value) noexcept
{
    auto *dst = reinterpret_cast<uint8_t *>(out);
    dst[7] = static_cast<uint8_t>(value);
    dst[6] = static_cast<uint8_t>(value >> 8);
    dst[5] = static_cast<uint8_t>(value >> 16);
    dst[4] = static_cast<uint8_t>(value >> 24);
    dst[3] = static_cast<uint8_t>(value >> 32);
    dst[2] = static_cast<uint8_t>(value >> 40);
    dst[1] = static_cast<uint8_t>(value >> 48);
    dst[0] = static_cast<uint8_t>(value >> 56);
}

inline auto put_uint64(MutBytes out, uint64_t value) noexcept
{
    put_uint64(out.data(), value);
}

} // cub

#endif // CUB_UTILS_ENCODING_H