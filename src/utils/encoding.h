/*
 * encoding.h: Simple serialization/deserialization procedures based on those found in LevelDB.
 */
#ifndef CALICO_UTILS_ENCODING_H
#define CALICO_UTILS_ENCODING_H

#include "calico/slice.h"
#include "types.h"

namespace Calico {

inline auto get_u16(const Byte *in) noexcept -> std::uint16_t
{
    const auto src = reinterpret_cast<const std::uint8_t *>(in);
    return static_cast<std::uint16_t>(src[0] | src[1] << 8);
}

inline auto get_u16(const Slice &in) noexcept -> std::uint16_t
{
    return get_u16(in.data());
}

inline auto get_u32(const Byte *in) noexcept -> std::uint32_t
{
    const auto src = reinterpret_cast<const std::uint8_t *>(in);
    return static_cast<std::uint32_t>(src[0]) |
           static_cast<std::uint32_t>(src[1]) << 8 |
           static_cast<std::uint32_t>(src[2]) << 16 |
           static_cast<std::uint32_t>(src[3]) << 24;
}

inline auto get_u32(const Slice &in) noexcept -> std::uint32_t
{
    return get_u32(in.data());
}

inline auto get_u64(const Byte *in) noexcept -> std::uint64_t
{
    const auto src = reinterpret_cast<const std::uint8_t *>(in);
    return static_cast<std::uint64_t>(src[0]) |
           static_cast<std::uint64_t>(src[1]) << 8 |
           static_cast<std::uint64_t>(src[2]) << 16 |
           static_cast<std::uint64_t>(src[3]) << 24 |
           static_cast<std::uint64_t>(src[4]) << 32 |
           static_cast<std::uint64_t>(src[5]) << 40 |
           static_cast<std::uint64_t>(src[6]) << 48 |
           static_cast<std::uint64_t>(src[7]) << 56;
}

inline auto get_u64(const Slice &in) noexcept -> std::uint64_t
{
    return get_u64(in.data());
}

inline auto put_u16(Byte *out, std::uint16_t value) noexcept -> void
{
    auto *dst = reinterpret_cast<std::uint8_t *>(out);
    dst[0] = static_cast<std::uint8_t>(value);
    dst[1] = static_cast<std::uint8_t>(value >> 8);
}

inline auto put_u16(Span out, std::uint16_t value) noexcept -> void
{
    put_u16(out.data(), value);
}

inline auto put_u32(Byte *out, std::uint32_t value) noexcept -> void
{
    auto *dst = reinterpret_cast<std::uint8_t *>(out);
    dst[0] = static_cast<std::uint8_t>(value);
    dst[1] = static_cast<std::uint8_t>(value >> 8);
    dst[2] = static_cast<std::uint8_t>(value >> 16);
    dst[3] = static_cast<std::uint8_t>(value >> 24);
}

inline auto put_u32(Span out, std::uint32_t value) noexcept -> void
{
    put_u32(out.data(), value);
}

inline auto put_u64(Byte *out, std::uint64_t value) noexcept -> void
{
    auto *dst = reinterpret_cast<std::uint8_t *>(out);
    dst[0] = static_cast<std::uint8_t>(value);
    dst[1] = static_cast<std::uint8_t>(value >> 8);
    dst[2] = static_cast<std::uint8_t>(value >> 16);
    dst[3] = static_cast<std::uint8_t>(value >> 24);
    dst[4] = static_cast<std::uint8_t>(value >> 32);
    dst[5] = static_cast<std::uint8_t>(value >> 40);
    dst[6] = static_cast<std::uint8_t>(value >> 48);
    dst[7] = static_cast<std::uint8_t>(value >> 56);
}

inline auto put_u64(Span out, std::uint64_t value) noexcept -> void
{
    put_u64(out.data(), value);
}

[[nodiscard]] inline auto varint_length(std::uint64_t value) -> Size
{
    Size length {1};
    while (value >= 0x80) {
        value >>= 7;
        ++length;
    }
    return length;
}

inline auto encode_varint(Byte *out, std::uint64_t value) -> Byte *
{
    auto *ptr = reinterpret_cast<std::uint8_t *>(out);
    while (value >= 0x80) {
        *ptr++ = std::uint8_t(value) | 0x80;
        value >>= 7;
    }
    *ptr++ = static_cast<std::uint8_t>(value);
    return reinterpret_cast<Byte *>(ptr);
}

inline auto decode_varint(const Byte *in, std::uint64_t &value) -> const Byte *
{
    value = 0;
    for (std::uint32_t shift {}; shift < 64; shift += 7) {
        std::uint64_t c = *reinterpret_cast<const std::uint8_t *>(in);
        ++in;
        if (c & 0x80) {
            value |= (c & 0x7F) << shift;
        } else {
            value |= c << shift;
            return in;
        }
    }
    return nullptr;
}

} // namespace Calico

#endif // CALICO_UTILS_ENCODING_H