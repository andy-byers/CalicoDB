// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_LOGGING_H
#define CALICODB_LOGGING_H

#include "buffer.h"
#include "encoding.h"
#include "internal.h"
#include "internal_string.h"
#include "unique_ptr.h"
#include <cstdarg>

namespace calicodb
{

class StringBuilder final
{
    // Buffer for accumulating string data. The length stored in the buffer type is the capacity,
    // and m_len is the number of bytes that have been written.
    String m_str;
    size_t m_len = 0;
    bool m_ok = true;

    // Make sure the underlying buffer is large enough to hold `len` bytes of string data, plus
    // a '\0'
    [[nodiscard]] auto ensure_capacity(size_t len) -> int;

public:
    explicit StringBuilder() = default;
    explicit StringBuilder(String str, size_t offset = 0);

    StringBuilder(StringBuilder &&rhs) noexcept
        : m_str(move(rhs.m_str)),
          m_len(exchange(rhs.m_len, 0U))
    {
    }

    auto operator=(StringBuilder &&rhs) noexcept -> StringBuilder &
    {
        if (this != &rhs) {
            m_str = move(rhs.m_str);
            m_len = exchange(rhs.m_len, 0U);
        }
        return *this;
    }

    [[nodiscard]] static auto release_string(String str) -> char *
    {
        str.m_size = 0;
        return exchange(str.m_data, nullptr);
    }

    auto append(const Slice &s) -> StringBuilder &;
    auto append(char c) -> StringBuilder &
    {
        return append(Slice(&c, 1));
    }
    auto append_format(const char *fmt, ...) -> StringBuilder &;
    auto append_format_va(const char *fmt, std::va_list args) -> StringBuilder &;
    auto append_escaped(const Slice &s) -> StringBuilder &;

    [[nodiscard]] auto build(String &string_out) -> int;
};

[[nodiscard]] auto append_strings(String &target, const Slice &s, const Slice &t = "") -> int;
[[nodiscard]] auto append_escaped_string(String &target, const Slice &s) -> int;
[[nodiscard]] auto append_format_string(String &target, const char *fmt, ...) -> int;
[[nodiscard]] auto append_format_string_va(String &target, const char *fmt, std::va_list args) -> int;
auto consume_decimal_number(Slice &data, uint64_t *val) -> bool;

} // namespace calicodb

#endif // CALICODB_LOGGING_H