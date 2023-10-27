// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "logging.h"
#include "calicodb/env.h"
#include "internal.h"

namespace calicodb
{

StringBuilder::StringBuilder(String str)
{
    if (auto *ptr = exchange(str.m_data, nullptr)) {
        m_buf.reset(ptr, str.m_size);
        m_len = str.m_size;
        str.clear();
    }
}

auto StringBuilder::ensure_capacity(size_t len) -> int
{
    if (!m_ok) {
        return -1;
    }
    // NOTE: This routine only fails if len_with_null is greater than kMaxAllocation
    //       ("capacity > kMaxAllocation" might end up being true below, but that is OK).
    const auto len_with_null = len + 1;
    if (len_with_null <= m_buf.len()) {
        return 0;
    }
    if (len_with_null > kMaxAllocation) {
        return -1;
    }
    auto capacity = m_buf.is_empty()
                        ? len + 1
                        : m_buf.len();
    while (len_with_null > capacity) {
        capacity *= 2;
    }
    if (m_buf.realloc(minval<size_t>(capacity, kMaxAllocation))) {
        m_ok = false;
        return -1;
    }
    return 0;
}

auto StringBuilder::build(String &string_out) -> int
{
    // Trim the allocation.
    if (m_ok && m_len + 1 < m_buf.len()) {
        m_ok = m_buf.realloc(m_len + 1) == 0;
    }
    if (!m_ok) {
        return -1;
    }
    if (!m_buf.is_empty()) {
        CALICODB_EXPECT_EQ(m_buf.len(), m_len + 1);
        m_buf[m_len] = '\0';
    }
    string_out = String(m_buf.release(), m_len);
    m_len = 0;
    return 0;
}

auto StringBuilder::append(const Slice &s) -> StringBuilder &
{
    if (ensure_capacity(m_len + s.size())) {
        return *this;
    }
    CALICODB_EXPECT_TRUE(m_ok);
    std::memcpy(m_buf.ptr() + m_len, s.data(), s.size());
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
        auto rc = std::vsnprintf(m_buf.ptr() + m_len,
                                 m_buf.len() - m_len,
                                 fmt, args_copy);
        va_end(args_copy);

        if (rc < 0) {
            // This should never happen.
            CALICODB_DEBUG_TRAP;
            break;
        }
        const auto len = m_len + static_cast<size_t>(rc);
        if (len + 1 <= m_buf.len()) {
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
    return StringBuilder(move(str))
        .append(s)
        .append(t)
        .build(str);
}

auto append_escaped_string(String &target, const Slice &s) -> int
{
    return StringBuilder(move(target))
        .append_escaped(s)
        .build(target);
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
    return StringBuilder(move(target))
        .append_format_va(fmt, args)
        .build(target);
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