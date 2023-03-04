#include "logging.h"
#include "utils.h"
#include <limits>

namespace calicodb
{

auto append_number(std::string &out, std::size_t value) -> void
{
    char buffer[30];
    std::snprintf(buffer, sizeof(buffer), "%llu", static_cast<unsigned long long>(value));
    out.append(buffer);
}

auto append_double(std::string &out, double value) -> void
{
    char buffer[30];
    std::snprintf(buffer, sizeof(buffer), "%g", value);
    out.append(buffer);
}

auto append_escaped_string(std::string &out, const Slice &value) -> void
{
    for (std::size_t i {}; i < value.size(); ++i) {
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

auto number_to_string(std::size_t value) -> std::string
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

// Modified from LevelDB.
auto consume_decimal_number(Slice &in, std::uint64_t &val) -> bool
{
    // Constants that will be optimized away.
    static constexpr const std::uint64_t kMaxUint64 = std::numeric_limits<std::uint64_t>::max();
    static constexpr const char kLastDigitOfMaxUint64 = '0' + static_cast<char>(kMaxUint64 % 10);

    std::uint64_t value {};

    // reinterpret_cast-ing from char* to uint8_t* to avoid signedness.
    const auto *start = reinterpret_cast<const std::uint8_t*>(in.data());

    const auto *end = start + in.size();
    const auto *current = start;
    for (; current != end; ++current) {
        const auto ch = *current;
        if (ch < '0' || ch > '9') break;

        // Overflow check.
        // kMaxUint64 / 10 is also constant and will be optimized away.
        if (value > kMaxUint64 / 10 || (value == kMaxUint64 / 10 && ch > kLastDigitOfMaxUint64)) {
            return false;
        }

        value = (value * 10) + (ch - '0');
    }

    val = value;
    CDB_EXPECT_GE(current, start);
    const auto digits_consumed = static_cast<std::size_t>(current - start);
    in.advance(digits_consumed);
    return digits_consumed != 0;
}

} // namespace calicodb