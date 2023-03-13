// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for contributor names.

#ifndef CALICODB_ENCODING_H
#define CALICODB_ENCODING_H

#include "calicodb/slice.h"
#include "types.h"

namespace calicodb
{

inline auto get_u16(const char *in) noexcept -> std::uint16_t
{
    const auto src = reinterpret_cast<const std::uint8_t *>(in);
    return static_cast<std::uint16_t>(src[0] | src[1] << 8);
}

inline auto get_u16(const Slice &in) noexcept -> std::uint16_t
{
    return get_u16(in.data());
}

inline auto get_u32(const char *in) noexcept -> std::uint32_t
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

inline auto get_u64(const char *in) noexcept -> std::uint64_t
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

inline auto put_u16(char *out, std::uint16_t value) noexcept -> void
{
    auto *dst = reinterpret_cast<std::uint8_t *>(out);
    dst[0] = static_cast<std::uint8_t>(value);
    dst[1] = static_cast<std::uint8_t>(value >> 8);
}

inline auto put_u16(Span out, std::uint16_t value) noexcept -> void
{
    put_u16(out.data(), value);
}

inline auto put_u32(char *out, std::uint32_t value) noexcept -> void
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

inline auto put_u64(char *out, std::uint64_t value) noexcept -> void
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

static constexpr std::size_t kVarintMaxLength {10};

[[nodiscard]] inline auto varint_length(std::uint64_t value) -> std::size_t
{
    std::size_t length {1};
    while (value >= 0x80) {
        value >>= 7;
        ++length;
    }
    return length;
}

inline auto encode_varint(char *out, std::uint64_t value) -> char *
{
    auto *ptr = reinterpret_cast<std::uint8_t *>(out);
    while (value >= 0x80) {
        *ptr++ = std::uint8_t(value) | 0x80;
        value >>= 7;
    }
    *ptr++ = static_cast<std::uint8_t>(value);
    return reinterpret_cast<char *>(ptr);
}

// TODO: We should pass an "end" variable to mark the end of possible input. This will help
//       catch corruption in data pages, and prevent out-of-bounds reads.
inline auto decode_varint(const char *in, std::uint64_t &value) -> const char *
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

} // namespace calicodb

#endif // CALICODB_ENCODING_H