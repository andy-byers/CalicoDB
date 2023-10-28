// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "logging.h"
#include "calicodb/env.h"
#include "internal.h"

namespace calicodb
{

StringBuilder::StringBuilder(String str, size_t offset)
{
    CALICODB_EXPECT_LE(offset, str.size());
    if (!str.is_empty()) {
        m_str = move(str);
        m_len = offset;
    }
}

auto StringBuilder::ensure_capacity(size_t len) -> int
{
    if (!m_ok) {
        return -1;
    }
    if (len <= m_str.size()) {
        // String already has enough memory.
        return 0;
    }
    auto capacity = m_str.is_empty() ? len + 1
                                     : m_str.size();
    while (capacity < len) {
        capacity *= 2;
    }
    if (m_str.resize(capacity)) {
        m_ok = false;
        return -1;
    }
    return 0;
}

auto StringBuilder::build(String &string_out) -> int
{
    // Trim the allocation.
    if (m_ok && m_len < m_str.size()) {
        m_ok = m_str.resize(m_len) == 0;
    }
    if (m_ok) {
        string_out = move(m_str);
        m_len = 0;
        return 0;
    }
    return -1;
}

auto StringBuilder::append(const Slice &s) -> StringBuilder &
{
    if (ensure_capacity(m_len + s.size())) {
        return *this;
    }
    CALICODB_EXPECT_TRUE(m_ok);
    std::memcpy(m_str.data() + m_len, s.data(), s.size());
    m_len += s.size();
    return *this;
}

auto StringBuilder::append_escaped(const Slice &s) -> StringBuilder &
{
    for (size_t i = 0; m_ok && i < s.size(); ++i) {
        const auto chr = s[i];
        if (chr >= ' ' && chr <= '~') {
            append(chr);
        } else {
            char buffer[10];
            std::snprintf(buffer, sizeof(buffer), "\\x%02X", static_cast<unsigned>(chr) & 0xFF);
            append(buffer);
        }
    }
    return *this;
}

auto StringBuilder::append_format(const char *fmt, ...) -> StringBuilder &
{
    std::va_list args;
    va_start(args, fmt);
    append_format_va(fmt, args);
    va_end(args);
    return *this;
}

auto StringBuilder::append_format_va(const char *fmt, std::va_list args) -> StringBuilder &
{
    // Make sure the pointer is not null for std::vsnprintf().
    if (ensure_capacity(1)) {
        return *this;
    }
    CALICODB_EXPECT_TRUE(m_ok);
    auto ok = false;
    for (int i = 0; i < 2; ++i) {
        std::va_list args_copy;
        va_copy(args_copy, args);
        // Add 1 to the size, since m_str.size() does not include the '\0'.
        const auto rc = std::vsnprintf(m_str.data() + m_len,
                                       m_str.size() - m_len + 1,
                                       fmt, args_copy);
        va_end(args_copy);

        if (rc < 0) {
            // This should never happen.
            CALICODB_DEBUG_TRAP;
            break;
        }
        const auto len = m_len + static_cast<size_t>(rc);
        if (len <= m_str.size()) {
            // Success: m_buf had enough room for the message + '\0'.
            m_len = len;
            ok = true;
            break;
        } else if (i) {
            // Already tried reallocating once. std::vsnprintf() may have a bug.
            CALICODB_DEBUG_TRAP;
            break;
        } else if (ensure_capacity(len)) {
            // Out of memory.
            break;
        }
    }
    m_ok = ok;
    return *this;
}

auto append_strings(String &str, const Slice &s, const Slice &t) -> int
{
    const auto offset = str.size();
    return StringBuilder(move(str), offset)
        .append(s)
        .append(t)
        .build(str);
}

auto append_escaped_string(String &str, const Slice &s) -> int
{
    const auto offset = str.size();
    return StringBuilder(move(str), offset)
        .append_escaped(s)
        .build(str);
}

auto append_format_string(String &str, const char *fmt, ...) -> int
{
    std::va_list args;
    va_start(args, fmt);
    const auto rc = append_format_string_va(str, fmt, args);
    va_end(args);
    return rc;
}

auto append_format_string_va(String &str, const char *fmt, std::va_list args) -> int
{
    const auto offset = str.size();
    return StringBuilder(move(str), offset)
        .append_format_va(fmt, args)
        .build(str);
}

// Modified from LevelDB.
auto consume_decimal_number(Slice &in, uint64_t *val) -> bool
{
    // Constants that will be optimized away.
    static constexpr const uint64_t kMaxUint64 = UINT64_MAX;
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