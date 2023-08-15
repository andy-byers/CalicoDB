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

auto append_fmt_string(UniqueBuffer &out, const char *fmt, ...) -> void;

auto append_fmt_string(std::string &out, const char *fmt, ...) -> void;
auto append_number(std::string &out, size_t value) -> void;
auto append_escaped_string(std::string &out, const Slice &value) -> void;
auto number_to_string(size_t value) -> std::string;
auto escape_string(const Slice &value) -> std::string;
auto consume_decimal_number(Slice &data, uint64_t *val) -> bool;

} // namespace calicodb

#endif // CALICODB_LOGGING_H