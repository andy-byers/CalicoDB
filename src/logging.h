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

namespace calicodb
{

class StringBuilder final
{
    // Buffer for accumulating string data. The length stored in the buffer type is the capacity,
    // and m_len is the number of bytes that have been written.
    Buffer<char> m_buf;
    size_t m_len = 0;
    bool m_ok = true;

    // Make sure the underlying buffer is large enough to hold `len` bytes of string data, plus
    // a '\0'
    [[nodiscard]] auto ensure_capacity(size_t len) -> int;

public:
    explicit StringBuilder() = default;
    explicit StringBuilder(String str);

    StringBuilder(StringBuilder &&rhs) noexcept
        : m_buf(move(rhs.m_buf)),
          m_len(exchange(rhs.m_len, 0U))
    {
    }

    auto operator=(StringBuilder &&rhs) noexcept -> StringBuilder &
    {
        if (this != &rhs) {
            m_buf = move(rhs.m_buf);
            m_len = exchange(rhs.m_len, 0U);
        }
        return *this;
    }

    [[nodiscard]] static auto release_string(String str) -> char *
    {
        str.m_len = 0;
        str.m_cap = 0;
        return exchange(str.m_ptr, nullptr);
    }

    auto trim() -> StringBuilder &;
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

class StatusBuilder final
{
    StringBuilder m_builder;
    const Status m_fallback;

public:
    explicit StatusBuilder(Status::Code code, Status::SubCode subc = Status::kNone)
        : m_fallback(code, subc)
    {
        static constexpr uint16_t kInitialRefcount = 1;
        char header[4] = {'\x00', '\x00', code, subc};
        std::memcpy(header, &kInitialRefcount, sizeof(kInitialRefcount));
        m_builder.append(Slice(header, sizeof(header)));
    }

    [[nodiscard]] auto append(const Slice &s) -> StatusBuilder &
    {
        m_builder.append(s);
        return *this;
    }

    [[nodiscard]] auto append(char c) -> StatusBuilder &
    {
        return append(Slice(&c, 1));
    }

    template <class... Args>
    [[nodiscard]] auto append_format(const char *fmt, Args &&...args) -> StatusBuilder &
    {
        m_builder.append_format(fmt, forward<Args>(args)...);
        return *this;
    }

    [[nodiscard]] auto append_escaped(const Slice &s) -> StatusBuilder &
    {
        m_builder.append(s);
        return *this;
    }

    auto build() -> Status
    {
        String string;
        if (m_builder.trim().build(string)) {
            return m_fallback;
        }
        return Status(StringBuilder::release_string(
            std::move(string)));
    }

    template <class... Args>
    static auto invalid_argument(const char *fmt, Args &&...args) -> Status
    {
        return StatusBuilder(Status::kInvalidArgument)
            .append_format(fmt, forward<Args>(args)...)
            .build();
    }

    template <class... Args>
    static auto not_supported(const char *fmt, Args &&...args) -> Status
    {
        return StatusBuilder(Status::kNotSupported)
            .append_format(fmt, forward<Args>(args)...)
            .build();
    }

    template <class... Args>
    static auto corruption(const char *fmt, Args &&...args) -> Status
    {
        return StatusBuilder(Status::kCorruption)
            .append_format(fmt, forward<Args>(args)...)
            .build();
    }

    template <class... Args>
    static auto not_found(const char *fmt, Args &&...args) -> Status
    {
        return StatusBuilder(Status::kNotFound)
            .append_format(fmt, forward<Args>(args)...)
            .build();
    }

    template <class... Args>
    static auto io_error(const char *fmt, Args &&...args) -> Status
    {
        return StatusBuilder(Status::kIOError)
            .append_format(fmt, forward<Args>(args)...)
            .build();
    }

    template <class... Args>
    static auto busy(const char *fmt, Args &&...args) -> Status
    {
        return StatusBuilder(Status::kBusy)
            .append_format(fmt, forward<Args>(args)...)
            .build();
    }

    template <class... Args>
    static auto aborted(const char *fmt, Args &&...args) -> Status
    {
        return StatusBuilder(Status::kAborted)
            .append_format(fmt, forward<Args>(args)...)
            .build();
    }

    template <class... Args>
    static auto retry(const char *fmt, Args &&...args) -> Status
    {
        return StatusBuilder(Status::kBusy, Status::kRetry)
            .append_format(fmt, forward<Args>(args)...)
            .build();
    }

    template <class... Args>
    static auto no_memory(const char *fmt, Args &&...args) -> Status
    {
        return StatusBuilder(Status::kAborted, Status::kNoMemory)
            .append_format(fmt, forward<Args>(args)...)
            .build();
    }
};

[[nodiscard]] auto append_strings(String &target, const Slice &s, const Slice &t = "") -> int;
[[nodiscard]] auto append_escaped_string(String &target, const Slice &s) -> int;
[[nodiscard]] auto append_format_string(String &target, const char *fmt, ...) -> int;
[[nodiscard]] auto append_format_string_va(String &target, const char *fmt, std::va_list args) -> int;
auto consume_decimal_number(Slice &data, uint64_t *val) -> bool;

} // namespace calicodb

#endif // CALICODB_LOGGING_H