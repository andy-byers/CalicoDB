// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "logging.h"
#include "calicodb/env.h"
#include "utils.h"
#include <limits>

namespace calicodb
{

StringBuilder::StringBuilder(String str)
    : m_len(0)
{
    if (auto *ptr = std::exchange(str.m_ptr, nullptr)) {
        m_buf.reset(ptr, str.m_cap);
        m_len = str.m_len;
        str.clear();
    }
}

auto StringBuilder::ensure_capacity(size_t len) -> int
{
    auto capacity = std::max<size_t>(m_buf.len(), 1);
    while (len + 1 > capacity) {
        capacity *= 2;
    }
    if (capacity > m_buf.len()) {
        return m_buf.realloc(capacity);
    }
    return 0;
}

auto StringBuilder::build() && -> String
{
    const auto capacity = m_buf.len();
    const auto length = std::exchange(m_len, 0);
    auto *pointer = m_buf.release();
    if (capacity) {
        CALICODB_EXPECT_NE(pointer, nullptr);
        CALICODB_EXPECT_LT(length, capacity);
        pointer[length] = '\0';
    }
    return String(pointer, length, capacity);
}

auto StringBuilder::append(const Slice &s) -> int
{
    if (ensure_capacity(m_len + s.size())) {
        return -1;
    }
    std::memcpy(m_buf.ptr() + m_len, s.data(), s.size());
    m_len += s.size();
    return 0;
}

auto StringBuilder::append_escaped(const Slice &s) -> int
{
    int rc = 0;
    for (size_t i = 0; rc == 0 && i < s.size(); ++i) {
        const auto chr = s[i];
        if (chr >= ' ' && chr <= '~') {
            rc = append(chr);
        } else {
            char buffer[10];
            std::snprintf(buffer, sizeof(buffer), "\\x%02x", static_cast<unsigned>(chr) & 0xFF);
            rc = append(buffer);
        }
    }
    return rc;
}

auto StringBuilder::append_format(const char *fmt, ...) -> int
{
    std::va_list args;
    va_start(args, fmt);
    const auto rc = append_format_va(fmt, args);
    va_end(args);
    return rc;
}

auto StringBuilder::append_format_va(const char *fmt, std::va_list args) -> int
{
    // Make sure the pointer is not null.
    if (ensure_capacity(1)) {
        return -1;
    }
    for (int i = 0; i < 2; ++i) {
        std::va_list args_copy;
        va_copy(args_copy, args);
        auto rc = std::vsnprintf(m_buf.ptr() + m_len,
                                 m_buf.len() - m_len,
                                 fmt, args_copy);
        va_end(args_copy);

        if (rc < 0) {
            // This should never happen.
            CALICODB_DEBUG_TRAP;
            return -1;
        }
        const auto len = m_len + static_cast<size_t>(rc);
        if (len + 1 <= m_buf.len()) {
            // Success: m_buf had enough room for the message + '\0'.
            m_len = len;
            break;
        } else if (i) {
            // Already tried reallocating once. std::vsnprintf() may have a bug.
            CALICODB_DEBUG_TRAP;
            return -1;
        } else if (ensure_capacity(len)) {
            // Out of memory.
            return -1;
        }
    }
    return 0;
}

auto append_strings(String &str, const Slice &s, const Slice &t) -> int
{
    StringBuilder builder(std::move(str));
    auto rc = builder.append(s);
    if (rc == 0) {
        rc = builder.append(t);
    }
    str = std::move(builder).build();
    return rc;
}

auto append_escaped_string(String &target, const Slice &s) -> int
{
    StringBuilder builder(std::move(target));
    const auto rc = builder.append_escaped(s);
    target = std::move(builder).build();
    return rc;
}

auto append_format_string(String &target, const char *fmt, ...) -> int
{
    std::va_list args;
    va_start(args, fmt);
    const auto rc = append_format_string_va(target, fmt, args);
    va_end(args);
    return rc;
}

auto append_format_string_va(String &target, const char *fmt, std::va_list args) -> int
{
    StringBuilder builder(std::move(target));
    const auto rc = builder.append_format_va(fmt, args);
    target = std::move(builder).build();
    return rc;
}

// Modified from LevelDB.
auto consume_decimal_number(Slice &in, uint64_t *val) -> bool
{
    // Constants that will be optimized away.
    static constexpr const uint64_t kMaxUint64 = std::numeric_limits<uint64_t>::max();
    static constexpr const char kLastDigitOfMaxUint64 = '0' + static_cast<char>(kMaxUint64 % 10);

    uint64_t value = 0;

    // reinterpret_cast-ing from char* to uint8_t* to avoid signedness.
    const auto *start = reinterpret_cast<const uint8_t *>(in.data());

    const auto *end = start + in.size();
    const auto *current = start;
    for (; current != end; ++current) {
        const auto ch = *current;
        if (ch < '0' || ch > '9')
            break;

        // Overflow check.
        // kMaxUint64 / 10 is also constant and will be optimized away.
        if (value > kMaxUint64 / 10 || (value == kMaxUint64 / 10 && ch > kLastDigitOfMaxUint64)) {
            return false;
        }

        value = (value * 10) + (ch - '0');
    }

    if (val != nullptr) {
        *val = value;
    }
    CALICODB_EXPECT_GE(current, start);
    const auto digits_consumed = static_cast<size_t>(current - start);
    in.advance(digits_consumed);
    return digits_consumed != 0;
}

} // namespace calicodb