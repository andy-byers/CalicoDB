#include "logging.h"

namespace calicodb
{

auto append_number(std::string &out, Size value) -> void
{
    Byte buffer[30];
    std::snprintf(buffer, sizeof(buffer), "%llu", static_cast<unsigned long long>(value));
    out.append(buffer);
}

auto append_double(std::string &out, double value) -> void
{
    Byte buffer[30];
    std::snprintf(buffer, sizeof(buffer), "%g", value);
    out.append(buffer);
}

auto append_escaped_string(std::string &out, const Slice &value) -> void
{
    for (Size i {}; i < value.size(); ++i) {
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

auto number_to_string(Size value) -> std::string
{
    std::string out;
    append_number(out, value);
    return out;
}

auto double_to_string(double value) -> std::string
{
    std::string out;
    append_double(out, value);
    return out;
}

auto escape_string(const Slice &value) -> std::string
{
    std::string out;
    append_escaped_string(out, value);
    return out;
}

} // namespace calicodb