// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_LOGGING_H
#define CALICODB_LOGGING_H

#include "calicodb/slice.h"
#include "ptr.h"
#include "utils.h"
#include <cstdarg>

namespace calicodb
{

class String;
[[nodiscard]] auto append_number(String &out, uint64_t value) -> int;
[[nodiscard]] auto append_fmt_string(String &out, const char *fmt, ...) -> int;
[[nodiscard]] auto append_fmt_string_va(String &out, const char *fmt, std::va_list args) -> int;

auto consume_decimal_number(Slice &data, uint64_t *val) -> bool;

} // namespace calicodb

#endif // CALICODB_LOGGING_H