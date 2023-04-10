// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_ENCODING_H
#define CALICODB_ENCODING_H

#include "utils.h"

namespace calicodb
{

inline auto get_u16(const char *in) noexcept -> U16
{
    const auto src = reinterpret_cast<const U8 *>(in);
    return static_cast<U16>(src[0] | src[1] << 8);
}

inline auto get_u16(const Slice &in) noexcept -> U16
{
    return get_u16(in.data());
}

inline auto get_u32(const char *in) noexcept -> U32
{
    const auto src = reinterpret_cast<const U8 *>(in);
    return static_cast<U32>(src[0]) |
           static_cast<U32>(src[1]) << 8 |
           static_cast<U32>(src[2]) << 16 |
           static_cast<U32>(src[3]) << 24;
}

inline auto get_u32(const Slice &in) noexcept -> U32
{
    return get_u32(in.data());
}

inline auto get_u64(const char *in) noexcept -> U64
{
    const auto src = reinterpret_cast<const U8 *>(in);
    return static_cast<U64>(src[0]) |
           static_cast<U64>(src[1]) << 8 |
           static_cast<U64>(src[2]) << 16 |
           static_cast<U64>(src[3]) << 24 |
           static_cast<U64>(src[4]) << 32 |
           static_cast<U64>(src[5]) << 40 |
           static_cast<U64>(src[6]) << 48 |
           static_cast<U64>(src[7]) << 56;
}

inline auto get_u64(const Slice &in) noexcept -> U64
{
    return get_u64(in.data());
}

inline auto put_u16(char *out, U16 value) noexcept -> void
{
    auto *dst = reinterpret_cast<U8 *>(out);
    dst[0] = static_cast<U8>(value);
    dst[1] = static_cast<U8>(value >> 8);
}

inline auto put_u32(char *out, U32 value) noexcept -> void
{
    auto *dst = reinterpret_cast<U8 *>(out);
    dst[0] = static_cast<U8>(value);
    dst[1] = static_cast<U8>(value >> 8);
    dst[2] = static_cast<U8>(value >> 16);
    dst[3] = static_cast<U8>(value >> 24);
}

inline auto put_u64(char *out, U64 value) noexcept -> void
{
    auto *dst = reinterpret_cast<U8 *>(out);
    dst[0] = static_cast<U8>(value);
    dst[1] = static_cast<U8>(value >> 8);
    dst[2] = static_cast<U8>(value >> 16);
    dst[3] = static_cast<U8>(value >> 24);
    dst[4] = static_cast<U8>(value >> 32);
    dst[5] = static_cast<U8>(value >> 40);
    dst[6] = static_cast<U8>(value >> 48);
    dst[7] = static_cast<U8>(value >> 56);
}

static constexpr std::size_t kVarintMaxLength = 10;

[[nodiscard]] inline auto varint_length(U64 value) -> std::size_t
{
    std::size_t length = 1;
    while (value >= 0x80) {
        value >>= 7;
        ++length;
    }
    return length;
}

inline auto encode_varint(char *out, U64 value) -> char *
{
    auto *ptr = reinterpret_cast<U8 *>(out);
    while (value >= 0x80) {
        *ptr++ = U8(value) | 0x80;
        value >>= 7;
    }
    *ptr++ = static_cast<U8>(value);
    return reinterpret_cast<char *>(ptr);
}

// TODO: We should pass an "end" variable to mark the end of possible input. This will help
//       catch corruption in data pages, and prevent out-of-bounds reads.
inline auto decode_varint(const char *in, U64 &value) -> const char *
{
    value = 0;
    for (U32 shift = 0; shift < 64; shift += 7) {
        U64 c = *reinterpret_cast<const U8 *>(in);
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