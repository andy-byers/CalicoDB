// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_LOGGING_H
#define CALICODB_LOGGING_H

#include "calicodb/slice.h"
#include "utils.h"
#include <cstdarg>

namespace calicodb
{

auto attempt_fmt(char *ptr, std::size_t length, bool append_newline, const char *fmt, std::va_list args) -> std::size_t;
auto append_fmt_string(std::string &out, const char *fmt, ...) -> std::size_t;
auto append_number(std::string &out, std::size_t value) -> void;
auto append_escaped_string(std::string &out, const Slice &value) -> void;
auto number_to_string(std::size_t value) -> std::string;
auto escape_string(const Slice &value) -> std::string;
auto consume_decimal_number(Slice &data, U64 *val) -> bool;

} // namespace calicodb

#endif // CALICODB_LOGGING_H