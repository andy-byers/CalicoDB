// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "logging.h"
#include "calicodb/env.h"
#include "utils.h"
#include <limits>

namespace calicodb
{

struct WriteInfo {
    std::size_t length = 0;
    bool success = false;
};

static auto try_write(char *buffer, std::size_t buffer_size, bool add_newline, const char *fmt, std::va_list args) -> WriteInfo
{
    std::va_list copy;
    va_copy(copy, args);
    const auto rc = std::vsnprintf(buffer, buffer_size, fmt, copy);
    va_end(copy);

    WriteInfo info;
    if (rc >= 0) {
        info.length = static_cast<std::size_t>(rc);
        if (info.length + 1 >= buffer_size) {
            // The message did not fit into the buffer.
            return {info.length + 2, false};
        }
        info.success = true;
        // Add a newline if necessary.
        if (add_newline && buffer[info.length - 1] != '\n') {
            buffer[info.length] = '\n';
            ++info.length;
        }
    }
    return info;
}

auto append_fmt_string(std::string &out, const char *fmt, ...) -> std::size_t
{
    std::va_list args;
    va_start(args, fmt);
    std::string buffer(32, '\0');
    auto info = try_write(buffer.data(), buffer.size(), false, fmt, args);
    buffer.resize(info.length);
    if (!info.success) {
        info = try_write(buffer.data(), buffer.size(), false, fmt, args);
    }
    va_end(args);
    out.append(buffer);
    return info.length;
}

auto logv(Sink *log, const char *fmt, ...) -> void
{
    if (log) {
        std::va_list args;
        va_start(args, fmt);

        // Try to fit the message in this stack buffer. If it won't fit, allocate space
        // for it on the heap.
        char fixed[128];
        auto *p = fixed;
        char *variable = nullptr;

        WriteInfo info;
        info.length = sizeof(fixed);
        for (int i = 0; i < 2; ++i) {
            info = try_write(p, info.length, true, fmt, args);
            if (info.success) {
                log->sink(Slice(p, info.length));
                break;
            } else if (i == 0) {
                variable = new char[info.length];
                p = variable;
            }
        }
        delete[] variable;
        va_end(args);
    }
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