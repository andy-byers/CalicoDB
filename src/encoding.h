// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_ENCODING_H
#define CALICODB_ENCODING_H

#include "utils.h"

namespace calicodb
{

inline auto get_u16(const char *ptr) -> U16
{
    const auto buf = reinterpret_cast<const U8 *>(ptr);
    return static_cast<U16>(buf[0]) |
           (static_cast<U16>(buf[1]) << 8);
}

inline auto get_u16(const Slice &slice) -> U16
{
    return get_u16(slice.data());
}

inline auto get_u32(const char *ptr) -> U32
{
    const auto buf = reinterpret_cast<const U8 *>(ptr);
    return static_cast<U32>(buf[0]) |
           (static_cast<U32>(buf[1]) << 8) |
           (static_cast<U32>(buf[2]) << 16) |
           (static_cast<U32>(buf[3]) << 24);
}

inline auto get_u32(const Slice &in) -> U32
{
    return get_u32(in.data());
}

inline auto get_u64(const char *ptr) -> U64
{
    const auto buf = reinterpret_cast<const U8 *>(ptr);
    return static_cast<U64>(buf[0]) |
           (static_cast<U64>(buf[1]) << 8) |
           (static_cast<U64>(buf[2]) << 16) |
           (static_cast<U64>(buf[3]) << 24) |
           (static_cast<U64>(buf[4]) << 32) |
           (static_cast<U64>(buf[5]) << 40) |
           (static_cast<U64>(buf[6]) << 48) |
           (static_cast<U64>(buf[7]) << 56);
}

inline auto get_u64(const Slice &slice) -> U64
{
    return get_u64(slice.data());
}

inline auto put_u16(char *ptr, U16 value) -> void
{
    auto *buf = reinterpret_cast<U8 *>(ptr);
    buf[0] = static_cast<U8>(value);
    buf[1] = static_cast<U8>(value >> 8);
}

inline auto put_u32(char *ptr, U32 value) -> void
{
    auto *buf = reinterpret_cast<U8 *>(ptr);
    buf[0] = static_cast<U8>(value);
    buf[1] = static_cast<U8>(value >> 8);
    buf[2] = static_cast<U8>(value >> 16);
    buf[3] = static_cast<U8>(value >> 24);
}

inline auto put_u64(char *ptr, U64 value) -> void
{
    auto *buf = reinterpret_cast<U8 *>(ptr);
    buf[0] = static_cast<U8>(value);
    buf[1] = static_cast<U8>(value >> 8);
    buf[2] = static_cast<U8>(value >> 16);
    buf[3] = static_cast<U8>(value >> 24);
    buf[4] = static_cast<U8>(value >> 32);
    buf[5] = static_cast<U8>(value >> 40);
    buf[6] = static_cast<U8>(value >> 48);
    buf[7] = static_cast<U8>(value >> 56);
}

static constexpr std::size_t kVarintMaxLength = 5;

[[nodiscard]] inline auto varint_length(U32 value) -> std::size_t
{
    std::size_t length = 1;
    while (value >= 0x80) {
        value >>= 7;
        ++length;
    }
    return length;
}

inline auto encode_varint(char *dst, U32 v) -> char *
{
    U8 *ptr = reinterpret_cast<U8 *>(dst);
    static constexpr int B = 128;
    if (v < (1 << 7)) {
        *ptr++ = v;
    } else if (v < (1 << 14)) {
        *ptr++ = v | B;
        *ptr++ = v >> 7;
    } else if (v < (1 << 21)) {
        *ptr++ = v | B;
        *ptr++ = (v >> 7) | B;
        *ptr++ = v >> 14;
    } else if (v < (1 << 28)) {
        *ptr++ = v | B;
        *ptr++ = (v >> 7) | B;
        *ptr++ = (v >> 14) | B;
        *ptr++ = v >> 21;
    } else {
        *ptr++ = v | B;
        *ptr++ = (v >> 7) | B;
        *ptr++ = (v >> 14) | B;
        *ptr++ = (v >> 21) | B;
        *ptr++ = v >> 28;
    }
    return reinterpret_cast<char *>(ptr);
}

inline auto decode_varint(const char *p, const char *limit, U32 &value_out) -> const char *
{
    U32 result = 0;
    for (U32 shift = 0; shift <= 28 && p < limit; shift += 7) {
        U32 byte = *reinterpret_cast<const U8 *>(p++);
        if (byte & 128) {
            // More bytes are present
            result |= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            value_out = result;
            return reinterpret_cast<const char *>(p);
        }
    }
    return nullptr;
}

} // namespace calicodb

#endif // CALICODB_ENCODING_H