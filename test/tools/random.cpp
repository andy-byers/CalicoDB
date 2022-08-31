#include "random.h"

namespace calico {

auto Random::next_string(Size size) -> std::string
{
    constexpr char chars[]{"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                           "abcdefghijklmnopqrstuvwxyz"
                           "0123456789"};
    auto result = std::string(static_cast<size_t>(size), '\x00');

    std::generate_n(result.begin(), size, [&]() -> char {
        // We need "sizeof(chars) - 2" to account for the '\0'. next_int() is
        // inclusive WRT its upper bound. Otherwise, we get random '\0's in
        // our string.
        return chars[next_int(sizeof(chars) - 2)];
    });
    return result;
}

auto Random::next_binary(Size size) -> std::string
{
    auto result = std::string(static_cast<size_t>(size), '\x00');

    std::generate_n(result.begin(), size, [&]() -> char {
        // We need "sizeof(chars) - 2" to account for the '\0'. next_int() is
        // inclusive WRT its upper bound. Otherwise, we get random '\0's in
        // our string.
        return static_cast<char>(next_int(0xFF));
    });
    return result;
}

} // namespace calico
