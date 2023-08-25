// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "error.h"
#include "alloc.h"
#include "logging.h"
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

    std::va_list args;
    va_start(args, code);
    m_errors[code].reset();
    const auto rc = append_fmt_string_va(m_errors[code], kErrorFmt[code], args);
    va_end(args);

    return rc ? "" : m_errors[code].c_str();
}

} // namespace calicodb