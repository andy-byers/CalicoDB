// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "logging.h"
#include "calicodb/env.h"
#include "utils.h"
#include <limits>

namespace calicodb
{

auto attempt_fmt(char *ptr, std::size_t length, bool append_newline, const char *fmt, std::va_list args) -> std::size_t
{
    std::va_list args_copy;
    va_copy(args_copy, args);
    const auto rc = std::vsnprintf(ptr, length, fmt, args);
    va_end(args_copy);

    // Assume that std::vsnprintf() will never fail.
    CALICODB_EXPECT_GE(rc, 0);
    auto write_length = static_cast<std::size_t>(rc);

    if (write_length + 1 >= length) {
        // The message did not fit into the buffer.
        return write_length + 2;
    }
    // Add a newline if necessary.
    if (append_newline && ptr[write_length - 1] != '\n') {
        ptr[write_length++] = '\n';
    }
    return write_length;
}

auto append_fmt_string(std::string &out, const char *fmt, ...) -> std::size_t
{
    // Write the formatted text at the end of `out`. First, try to format the text into 32 bytes of
    // additional memory. If that doesn't work, try again with the exact size needed.
    const auto offset = out.size();
    out.resize(offset + 32);

    std::va_list args;
    va_start(args, fmt);

    std::size_t added_length;
    for (int i = 0; i < 2; ++i) {
        added_length = attempt_fmt(
            out.data() + offset,
            out.size() - offset,
            false, fmt, args);
        const auto limit = out.size();
        out.resize(offset + added_length);
        if (offset + added_length <= limit) {
            break;
        }
    }
    va_end(args);
    return added_length;
}

auto append_number(std::string &out, std::size_t value) -> void
{
    char buffer[30];
    std::snprintf(buffer, sizeof(buffer), "%llu", static_cast<unsigned long long>(value));
    out.append(buffer);
}

auto append_double(std::string &out, double value) -> void
{
    char buffer[30];
    std::snprintf(buffer, sizeof(buffer), "%g", value);
    out.append(buffer);
}

auto append_escaped_string(std::string &out, const Slice &value) -> void
{
    for (std::size_t i = 0; i < value.size(); ++i) {
        const auto chr = value[i];
        if (chr >= ' ' && chr <= '~') {
            out.push_back(chr);
        } else {
            char buffer[10];
            std::snprintf(buffer, sizeof(buffer), "\\x%02x", static_cast<unsigned>(chr) & 0xFF);
            out.append(buffer);
        }
    }
}

auto number_to_string(std::size_t value) -> std::string
{
    std::string out;
    append_number(out, value);
    return out;
}

auto double_to_string(double value) -> std::string
{
    std::string out;
    append_double(out, value);
    return out;
}

auto escape_string(const Slice &value) -> std::string
{
    std::string out;
    append_escaped_string(out, value);
    return out;
}

// Modified from LevelDB.
auto consume_decimal_number(Slice &in, U64 *val) -> bool
{
    // Constants that will be optimized away.
    static constexpr const U64 kMaxUint64 = std::numeric_limits<U64>::max();
    static constexpr const char kLastDigitOfMaxUint64 = '0' + static_cast<char>(kMaxUint64 % 10);

    U64 value = 0;

    // reinterpret_cast-ing from char* to uint8_t* to avoid signedness.
    const auto *start = reinterpret_cast<const U8 *>(in.data());

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
    const auto digits_consumed = static_cast<std::size_t>(current - start);
    in.advance(digits_consumed);
    return digits_consumed != 0;
}

} // namespace calicodb