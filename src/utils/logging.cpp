#include "logging.h"
#include "calico/storage.h"
#include <cstdarg>

namespace Calico {

auto append_number(std::string &out, Size value) -> void
{
    Byte buffer[30];
    std::snprintf(buffer, sizeof(buffer), "%llu", static_cast<unsigned long long>(value));
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

auto escape_string(const Slice &value) -> std::string
{
    std::string out;
    append_escaped_string(out, value);
    return out;
}

} // namespace Calico