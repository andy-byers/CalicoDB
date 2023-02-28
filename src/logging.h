#ifndef CALICODB_LOGGING_H
#define CALICODB_LOGGING_H

#include "calicodb/slice.h"

namespace calicodb
{

auto append_number(std::string &out, std::size_t value) -> void;
auto append_double(std::string &out, double value) -> void;
auto append_escaped_string(std::string &out, const Slice &value) -> void;
auto number_to_string(std::size_t value) -> std::string;
auto double_to_string(double value) -> std::string;
auto escape_string(const Slice &value) -> std::string;

} // namespace calicodb

#endif // CALICODB_LOGGING_H