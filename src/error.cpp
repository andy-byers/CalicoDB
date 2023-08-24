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
    const auto old_len = error.is_empty() ? 0 : error.len() + 1;
    auto rc = std::vsnprintf(error.ptr(), old_len, fmt, args_copy);
    va_end(args_copy);
    // This code does not handle std::vsnprintf() failures.
    CALICODB_EXPECT_GE(rc, 0);
    const auto len = static_cast<size_t>(rc);

    if (len > error.len()) {
        // Make sure the buffer has enough space to fit the error message.
        error.resize(len);
        if (error.is_empty()) {
            return "out of memory in ErrorState::format_error()";
        }
        std::vsnprintf(error.ptr(), error.len() + 1, fmt, args);
    }
    va_end(args);
    return error.ptr();
}

} // namespace calicodb