// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "error.h"
#include "alloc.h"
#include <cstdarg>

namespace calicodb
{

static constexpr const char *kErrorFmt[] = {
    "corruption detected on %s with ID %u",
};

ErrorState::~ErrorState()
{
    for (const auto &[buf, _] : m_errors) {
        Alloc::free(buf);
    }
}

auto ErrorState::format_error(ErrorCode code, ...) -> const char *
{
    CALICODB_EXPECT_LE(code, 0);
    CALICODB_EXPECT_LT(code, kNumCodes);
    const char *fmt = kErrorFmt[code];
    auto &error = m_errors[code];

    std::va_list args;
    va_start(args, code);

    std::va_list args_copy;
    va_copy(args_copy, args);
    auto rc = std::vsnprintf(error.buf, error.len, fmt, args_copy);
    va_end(args_copy);
    // This code does not handle std::vsnprintf() failures.
    CALICODB_EXPECT_GE(rc, 0);
    const auto len = static_cast<size_t>(rc) + 1;

    if (len > error.len) {
        // Make sure the buffer has enough space to fit the error message.
        if (auto *buf = Alloc::realloc_string(error.buf, len)) {
            error.buf = buf;
            error.len = len;
        } else {
            return "out of memory in ErrorState::format_error()";
        }
        rc = std::vsnprintf(error.buf, error.len, fmt, args);

        CALICODB_EXPECT_GE(rc, 0);
        CALICODB_EXPECT_EQ(rc + 1, static_cast<int>(len));
    }
    va_end(args);
    return error.buf;
}

} // namespace calicodb