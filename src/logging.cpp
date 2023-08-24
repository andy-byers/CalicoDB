// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "logging.h"
#include "calicodb/env.h"
#include "utils.h"
#include <limits>

namespace calicodb
{

auto append_fmt_string(UniqueString &str, const char *fmt, ...) -> void
{
    std::va_list args;
    va_start(args, fmt);

    std::va_list args_copy;
    va_copy(args_copy, args);
    auto len = std::vsnprintf(nullptr, 0, fmt, args_copy);
    va_end(args_copy);

    CALICODB_EXPECT_GE(len, 0);
    auto offset = str.len();
    str.resize(offset + static_cast<size_t>(len));
    if (!str.is_empty()) {
        std::vsnprintf(str.ptr() + offset, str.len() - offset, fmt, args);
    }
    va_end(args);
}

auto append_fmt_string(std::string &str, const char *fmt, ...) -> void
{
    std::va_list args;
    va_start(args, fmt);

    std::va_list args_copy;
    va_copy(args_copy, args);
    auto len = std::vsnprintf(
        nullptr, 0,
        fmt, args_copy);
    va_end(args_copy);

    CALICODB_EXPECT_GE(len, 0);
    const auto offset = str.size();
    str.resize(offset + static_cast<size_t>(len) + 1);
    len = std::vsnprintf(
        str.data() + offset,
        str.size() - offset,
        fmt, args);
    str.pop_back();
    va_end(args);

    CALICODB_EXPECT_TRUE(0 <= len && len <= int(str.size()));
}

auto append_number(std::string &out, size_t value) -> void
{
    char buffer[30];
    std::snprintf(buffer, sizeof(buffer), "%llu", static_cast<unsigned long long>(value));
    out.append(buffer);
}

auto append_escaped_string(std::string &out, const Slice &value) -> void
{
    for (size_t i = 0; i < value.size(); ++i) {
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

auto number_to_string(size_t value) -> std::string
{
    std::string out;
    append_number(out, value);
    return out;
}

auto escape_string(const Slice &value) -> std::string
{
    std::string out;
    append_escaped_string(out, value);
    return out;
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