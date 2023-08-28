// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_LOGGING_H
#define CALICODB_LOGGING_H

#include "calicodb/string.h"
#include "encoding.h"
#include "ptr.h"
#include "utils.h"
#include <cstdarg>
#include <optional>

namespace calicodb
{

class StringBuilder final
{
    // Buffer for accumulating string data. The length stored in the buffer type is the capacity,
    // and m_len is the number of bytes that have been written.
    UniqueBuffer<char> m_buf;
    size_t m_len;

    // Make sure the underlying buffer is large enough to hold `len` bytes of string data, plus
    // a '\0'
    [[nodiscard]] auto ensure_capacity(size_t len) -> int;

public:
    explicit StringBuilder()
        : m_len(0)
    {
    }

    explicit StringBuilder(String str);

    StringBuilder(StringBuilder &&rhs) noexcept
        : m_buf(std::move(rhs.m_buf)),
          m_len(std::exchange(rhs.m_len, 0))
    {
    }

    auto operator=(StringBuilder &&rhs) noexcept -> StringBuilder &
    {
        if (this != &rhs) {
            m_buf = std::move(rhs.m_buf);
            m_len = std::exchange(rhs.m_len, 0);
        }
        return *this;
    }

    [[nodiscard]] auto trim() -> int;
    [[nodiscard]] auto build() && -> String;
    [[nodiscard]] auto append(const Slice &s) -> int;
    [[nodiscard]] auto append(char c) -> int
    {
        return append(Slice(&c, 1));
    }
    [[nodiscard]] auto append_format(const char *fmt, ...) -> int;
    [[nodiscard]] auto append_format_va(const char *fmt, std::va_list args) -> int;
    [[nodiscard]] auto append_escaped(const Slice &s) -> int;

    [[nodiscard]] static auto release_string(String str) -> char *
    {
        str.m_len = 0;
        str.m_cap = 0;
        return std::exchange(str.m_ptr, nullptr);
    }
};

class StatusBuilder final
{
public:
    [[nodiscard]] static auto initialize(StringBuilder &builder, Status::Code code, Status::SubCode subc = Status::kNone) -> int
    {
        char header[4] = {code, subc, '\x00', '\x00'};
        return builder.append(Slice(header, sizeof(header)));
    }

    [[nodiscard]] static auto finalize(StringBuilder builder) -> Status
    {
        return Status(StringBuilder::release_string(
            std::move(builder).build()));
    }

    [[nodiscard]] static auto compressed(Status::Code code, Status::SubCode subc = Status::kNone) -> Status
    {
        return Status(code, subc);
    }
};

[[nodiscard]] auto append_strings(String &target, const Slice &s, const Slice &t = "") -> int;
[[nodiscard]] auto append_escaped_string(String &target, const Slice &s) -> int;
[[nodiscard]] auto append_format_string(String &target, const char *fmt, ...) -> int;
[[nodiscard]] auto append_format_string_va(String &target, const char *fmt, std::va_list args) -> int;
auto consume_decimal_number(Slice &data, uint64_t *val) -> bool;

} // namespace calicodb

#endif // CALICODB_LOGGING_H