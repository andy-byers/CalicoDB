// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_ENCODING_H
#define CALICODB_ENCODING_H

#include "internal.h"

namespace calicodb
{

inline auto get_u16(const char *ptr) -> uint16_t
{
    const auto buf = reinterpret_cast<const uint8_t *>(ptr);
    return static_cast<uint16_t>(buf[0] | (buf[1] << 8));
}

inline auto get_u16(const Slice &slice) -> uint16_t
{
    return get_u16(slice.data());
}

inline auto get_u32(const char *ptr) -> uint32_t
{
    const auto buf = reinterpret_cast<const uint8_t *>(ptr);
    return static_cast<uint32_t>(buf[0]) |
           (static_cast<uint32_t>(buf[1]) << 8) |
           (static_cast<uint32_t>(buf[2]) << 16) |
           (static_cast<uint32_t>(buf[3]) << 24);
}

inline auto get_u32(const Slice &in) -> uint32_t
{
    return get_u32(in.data());
}

inline auto get_u64(const char *ptr) -> uint64_t
{
    const auto buf = reinterpret_cast<const uint8_t *>(ptr);
    return static_cast<uint64_t>(buf[0]) |
           (static_cast<uint64_t>(buf[1]) << 8) |
           (static_cast<uint64_t>(buf[2]) << 16) |
           (static_cast<uint64_t>(buf[3]) << 24) |
           (static_cast<uint64_t>(buf[4]) << 32) |
           (static_cast<uint64_t>(buf[5]) << 40) |
           (static_cast<uint64_t>(buf[6]) << 48) |
           (static_cast<uint64_t>(buf[7]) << 56);
}

inline auto get_u64(const Slice &slice) -> uint64_t
{
    return get_u64(slice.data());
}

inline auto put_u16(char *ptr, uint16_t value) -> void
{
    auto *buf = reinterpret_cast<uint8_t *>(ptr);
    buf[0] = static_cast<uint8_t>(value);
    buf[1] = static_cast<uint8_t>(value >> 8);
}

inline auto put_u32(char *ptr, uint32_t value) -> void
{
    auto *buf = reinterpret_cast<uint8_t *>(ptr);
    buf[0] = static_cast<uint8_t>(value);
    buf[1] = static_cast<uint8_t>(value >> 8);
    buf[2] = static_cast<uint8_t>(value >> 16);
    buf[3] = static_cast<uint8_t>(value >> 24);
}

inline auto put_u64(char *ptr, uint64_t value) -> void
{
    auto *buf = reinterpret_cast<uint8_t *>(ptr);
    buf[0] = static_cast<uint8_t>(value);
    buf[1] = static_cast<uint8_t>(value >> 8);
    buf[2] = static_cast<uint8_t>(value >> 16);
    buf[3] = static_cast<uint8_t>(value >> 24);
    buf[4] = static_cast<uint8_t>(value >> 32);
    buf[5] = static_cast<uint8_t>(value >> 40);
    buf[6] = static_cast<uint8_t>(value >> 48);
    buf[7] = static_cast<uint8_t>(value >> 56);
}

static constexpr size_t kVarintMaxLength = 5;

[[nodiscard]] inline auto varint_length(uint32_t value) -> size_t
{
    size_t length = 1;
    while (value >= 0x80) {
        value >>= 7;
        ++length;
    }
    return length;
}

inline auto encode_varint(char *dst, uint32_t v) -> char *
{
    auto *ptr = reinterpret_cast<uint8_t *>(dst);
    static constexpr int B = 128;
    if (v < (1 << 7)) {
        *ptr++ = static_cast<uint8_t>(v);
    } else if (v < (1 << 14)) {
        *ptr++ = static_cast<uint8_t>(v | B);
        *ptr++ = static_cast<uint8_t>(v >> 7);
    } else if (v < (1 << 21)) {
        *ptr++ = static_cast<uint8_t>(v | B);
        *ptr++ = static_cast<uint8_t>((v >> 7) | B);
        *ptr++ = static_cast<uint8_t>(v >> 14);
    } else if (v < (1 << 28)) {
        *ptr++ = static_cast<uint8_t>(v | B);
        *ptr++ = static_cast<uint8_t>((v >> 7) | B);
        *ptr++ = static_cast<uint8_t>((v >> 14) | B);
        *ptr++ = static_cast<uint8_t>(v >> 21);
    } else {
        *ptr++ = static_cast<uint8_t>(v | B);
        *ptr++ = static_cast<uint8_t>((v >> 7) | B);
        *ptr++ = static_cast<uint8_t>((v >> 14) | B);
        *ptr++ = static_cast<uint8_t>((v >> 21) | B);
        *ptr++ = static_cast<uint8_t>(v >> 28);
    }
    return reinterpret_cast<char *>(ptr);
}

inline auto decode_varint(const char *p, const char *limit, uint32_t &value_out) -> const char *
{
    uint32_t result = 0;
    for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
        uint32_t byte = *reinterpret_cast<const uint8_t *>(p++);
        if (byte & 128) {
            // More bytes are present
            result |= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            value_out = result;
            return p;
        }
    }
    return nullptr;
}

} // namespace calicodb

#endif // CALICODB_ENCODING_H