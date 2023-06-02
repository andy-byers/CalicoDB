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

inline auto encode_varint(char *ptr, U64 value) -> char *
{
    auto *buf = reinterpret_cast<U8 *>(ptr);
    while (value >= 0x80) {
        *buf++ = U8(value) | 0x80;
        value >>= 7;
    }
    *buf++ = static_cast<U8>(value);
    return reinterpret_cast<char *>(buf);
}

// TODO: We should pass an "end" variable to mark the end of possible input. This will help
//       catch corruption in data pages, and prevent out-of-bounds reads.
inline auto decode_varint(const char *ptr, U64 &value) -> const char *
{
    value = 0;
    for (U32 shift = 0; shift < 64; shift += 7) {
        U64 c = *reinterpret_cast<const U8 *>(ptr);
        ++ptr;
        if (c & 0x80) {
            value |= (c & 0x7F) << shift;
        } else {
            value |= c << shift;
            return ptr;
        }
    }
    return nullptr;
}
// TODO: Use this one instead.
inline auto decode_varint(const char *ptr, const char *end, U64 &value) -> const char *
{
    value = 0;
    for (U32 shift = 0; shift < 64 && ptr < end; shift += 7) {
        U64 c = *reinterpret_cast<const U8 *>(ptr);
        ++ptr;
        if (c & 0x80) {
            value |= (c & 0x7F) << shift;
        } else {
            value |= c << shift;
            return ptr;
        }
    }
    return nullptr;
}

} // namespace calicodb

#endif // CALICODB_ENCODING_H