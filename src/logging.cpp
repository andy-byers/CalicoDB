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
    const auto [data, size, capacity] = String::into_raw_parts(move(str));
    CALICODB_EXPECT_LE(offset, size);
    m_data.reset({data, capacity});
    m_size = offset;
}

auto StringBuilder::ensure_capacity(size_t len) -> int
{
    if (!m_ok) {
        return -1;
    }
    ++len; // Account for the null terminator.
    if (len <= m_data.size()) {
        // String already has enough memory.
        return 0;
    }
    // Make sure the capacity (unsigned) isn't going to wrap and become small.
    if (len > kMaxAllocation) {
        return -1;
    }
    size_t capacity = 4;
    while (capacity < len) {
        capacity *= 2;
    }
    if (m_data.resize(capacity)) {
        m_ok = false;
        return -1;
    }
    return 0;
}

auto StringBuilder::build(String &string_out) -> int
{
    const auto [data, capacity] = move(m_data).release();
    const auto size = exchange(m_size, 0U);
    const auto ok = exchange(m_ok, true);
    if (ok && size) {
        string_out = String::from_raw_parts({data, size, capacity});
        data[size] = '\0';
        return 0;
    }
    Mem::deallocate(data);
    string_out = String();
    return ok ? 0 : -1;
}

auto StringBuilder::append(const Slice &s) -> StringBuilder &
{
    // Empty check prevents allocating a null terminator if no data is to be added.
    if (s.is_empty() || ensure_capacity(m_size + s.size())) {
        return *this;
    }
    CALICODB_EXPECT_TRUE(m_ok);
    std::memcpy(m_data.data() + m_size, s.data(), s.size());
    m_size += s.size();
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
        const auto rc = std::vsnprintf(m_data.data() + m_size,
                                       m_data.size() - m_size,
                                       fmt, args_copy);
        va_end(args_copy);

        if (rc < 0) {
            // This should never happen.
            CALICODB_DEBUG_TRAP;
            break;
        }
        const auto size = m_size + static_cast<size_t>(rc);
        if (size + 1 <= m_data.size()) {
            // Success: m_buf had enough room for the message + '\0'.
            m_size = size;
            ok = true;
            break;
        } else if (i) {
            // Already tried reallocating once. std::vsnprintf() may have a bug.
            CALICODB_DEBUG_TRAP;
            break;
        } else if (ensure_capacity(size)) {
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