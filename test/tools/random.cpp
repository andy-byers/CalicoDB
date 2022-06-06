#include "random.h"

namespace cub {

/**
 * Create a random number generator with a default seed.
 */
Random::Random()
    : Random(0) { }

/**
 * Create and seed a random number generator.
 *
 * @param seed The RNG seed.
 */
Random::Random(Seed seed)
{
    set_seed(seed);
}

auto Random::engine() -> Engine
{
    return m_rng;
}

/**
 * Get the RNG seed.
 *
 * @return RNG seed.
 */
auto Random::seed() const -> Seed
{
    return m_seed;
}

/**
 * Set the RNG seed.
 *
 * @param seed RNG seed.
 */
auto Random::set_seed(Seed seed) -> void
{
    m_rng.seed(seed);
    m_seed = seed;
}

/**
 * Generate a random string.
 *
 * @param size The length of the string.
 * @return A string containing alphanumeric characters of the specified length.
 */
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

} // db
