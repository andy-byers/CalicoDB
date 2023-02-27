#ifndef CALICO_UTILS_LOGGING_H
#define CALICO_UTILS_LOGGING_H

#include "calico/slice.h"

namespace Calico {

auto append_number(std::string &out, Size value) -> void;
auto append_double(std::string &out, double value) -> void;
auto append_escaped_string(std::string &out, const Slice &value) -> void;
auto number_to_string(Size value) -> std::string;
auto double_to_string(double value) -> std::string;
auto escape_string(const Slice &value) -> std::string;

} // namespace Calico

#endif // CALICO_UTILS_LOGGING_H