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
        return exchange(str.m_ptr, nullptr);
    }
};

class StatusBuilder final
{
public:
    template <class... Args>
    static auto build(Status::Code code, Status::SubCode subc, const char *fmt, Args &&...args) -> Status
    {
        Status fallback(code, subc);

        StringBuilder builder;
        if (start(builder, code, subc)) {
            return fallback;
        }
        if (builder.append_format(fmt, forward<Args>(args)...)) {
            return fallback;
        }
        return finish(move(builder));
    }

    template <class... Args>
    static auto start(StringBuilder &builder, Status::Code code, Status::SubCode subc = Status::kNone) -> int
    {
        static constexpr uint16_t kInitialRefcount = 1;
        char header[4] = {'\x00', '\x00', code, subc};
        std::memcpy(header, &kInitialRefcount, sizeof(kInitialRefcount));
        return builder.append(Slice(header, sizeof(header)));
    }

    static auto finish(StringBuilder builder) -> Status
    {
        return Status(StringBuilder::release_string(
            move(builder).build()));
    }

    template <class... Args>
    static auto invalid_argument(const char *fmt, Args &&...args) -> Status
    {
        return build(Status::kInvalidArgument, Status::kNone, fmt, forward<Args>(args)...);
    }

    template <class... Args>
    static auto not_supported(const char *fmt, Args &&...args) -> Status
    {
        return build(Status::kNotSupported, Status::kNone, fmt, forward<Args>(args)...);
    }

    template <class... Args>
    static auto corruption(const char *fmt, Args &&...args) -> Status
    {
        return build(Status::kCorruption, Status::kNone, fmt, forward<Args>(args)...);
    }

    template <class... Args>
    static auto not_found(const char *fmt, Args &&...args) -> Status
    {
        return build(Status::kNotFound, Status::kNone, fmt, forward<Args>(args)...);
    }

    template <class... Args>
    static auto io_error(const char *fmt, Args &&...args) -> Status
    {
        return build(Status::kIOError, Status::kNone, fmt, forward<Args>(args)...);
    }

    template <class... Args>
    static auto busy(const char *fmt, Args &&...args) -> Status
    {
        return build(Status::kBusy, Status::kNone, fmt, forward<Args>(args)...);
    }

    template <class... Args>
    static auto aborted(const char *fmt, Args &&...args) -> Status
    {
        return build(Status::kAborted, Status::kNone, fmt, forward<Args>(args)...);
    }

    template <class... Args>
    static auto retry(const char *fmt, Args &&...args) -> Status
    {
        return build(Status::kBusy, Status::kRetry, fmt, forward<Args>(args)...);
    }

    template <class... Args>
    static auto no_memory(const char *fmt, Args &&...args) -> Status
    {
        return build(Status::kAborted, Status::kNoMemory, fmt, forward<Args>(args)...);
    }
};

[[nodiscard]] auto append_strings(String &target, const Slice &s, const Slice &t = "") -> int;
[[nodiscard]] auto append_escaped_string(String &target, const Slice &s) -> int;
[[nodiscard]] auto append_format_string(String &target, const char *fmt, ...) -> int;
[[nodiscard]] auto append_format_string_va(String &target, const char *fmt, std::va_list args) -> int;
auto consume_decimal_number(Slice &data, uint64_t *val) -> bool;

} // namespace calicodb

#endif // CALICODB_LOGGING_H