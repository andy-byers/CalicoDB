// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "logging.h"
#include "calicodb/db.h"
#include "calicodb/env.h"
#include "utils.h"
#include <limits>

namespace calicodb
{

auto append_fmt_string(String &str, const char *fmt, ...) -> int
{
    std::va_list args;
    va_start(args, fmt);
    const auto rc = append_fmt_string_va(str, fmt, args);
    va_end(args);
    return rc;
}

auto append_fmt_string_va(String &str, const char *fmt, std::va_list args) -> int
{
    std::va_list args_copy;
    va_copy(args_copy, args);
    auto rc = std::vsnprintf(nullptr, 0, fmt, args_copy);
    va_end(args_copy);

    if (rc < 0) {
        CALICODB_EXPECT_TRUE(false && "std::vsnprintf(nullptr, 0, ...) failed");
        return -1;
    }
    const auto len = static_cast<size_t>(rc);
    const auto end = std::strlen(str.c_str());
    if (realloc_string(str, end + len)) {
        return -1;
    }
    // realloc_string() adds a '\0'.
    rc = std::vsnprintf(str.data() + end, len + 1, fmt, args);

    if (rc < 0) {
        CALICODB_EXPECT_TRUE(false && "std::vsnprintf(ptr, len, ...) failed");
        return -1;
    }
    return 0;
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

auto append_number(String &out, uint64_t value) -> int
{
    char buf[30];
    auto rc = std::snprintf(buf, sizeof(buf), "%llu", static_cast<unsigned long long>(value));
    if (rc < 0) {
        CALICODB_DEBUG_TRAP;
        rc = 0;
    }
    const auto end = std::strlen(out.c_str());
    const auto len = static_cast<size_t>(rc);
    if (realloc_string(out, end + len)) {
        return -1;
    }
    std::memcpy(out.data() + end, buf, len);
    return 0;
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