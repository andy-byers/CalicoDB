#ifndef CCO_TEST_TOOLS_RANDOM_H
#define CCO_TEST_TOOLS_RANDOM_H

#include <algorithm>
#include <random>
#include "calico/options.h"
#include "utils/expect.h"

namespace cco {

/**
** Helper class for generating random numbers.
*/
class Random {
public:
    using Engine = std::default_random_engine;
    using Seed = uint32_t;

    Random();
    explicit Random(Seed);
    virtual ~Random() = default;
    auto seed() const -> Seed;
    auto set_seed(Seed) -> void;
    auto next_string(Size) -> std::string;
    auto next_binary(Size) -> std::string;
    template<class C> auto shuffle(C&) -> void;
    template<class T> auto next_int(T) -> T;
    template<class T> auto next_int(T, T) -> T;
    template<class T> auto next_real(T) -> T;
    template<class T> auto next_real(T, T) -> T;

protected:
    Seed m_seed {};  ///< Seed for the random number generator.
    std::default_random_engine m_rng; ///< STL random number generation engine.
};

template<class Container> auto Random::shuffle(Container &data) -> void
{
    std::shuffle(begin(data), end(data), m_rng);
}

/**
** Generate a positive integer from a uniform distribution.
**
** @param v_max The maximum value (inclusive) that the integer can take.
** @return A random integer in the view [0, v_max].
*/
template<class T> auto Random::next_int(T v_max) -> T
{
    return next_int(T {}, v_max);
}

/**
** Generate an integer from a uniform distribution.
**
** @param v_min The minimum value (inclusive) that the integer can take.
** @param v_max The maximum value (inclusive) that the integer can take.
** @return A random integer in the view [v_min, v_max].
*/
template<class T> auto Random::next_int(T v_min, T v_max) -> T
{
    CCO_EXPECT_LE(v_min, v_max);
    std::uniform_int_distribution<T> dist(v_min, v_max);
    return dist(m_rng);
}

/**
** Generate a positive real number from a uniform distribution.
**
** @param v_max The maximum value (inclusive) that the number can take.
** @return A random real number in the view [0, v_max].
*/
template<class T> auto Random::next_real(T v_max) -> T
{
    return next_real(T {}, v_max);
}

/**
** Generate a real number from a uniform distribution.
**
** @param v_min The minimum value (inclusive) that the number can take.
** @param v_max The maximum value (inclusive) that the number can take.
** @return A random real number in the view [v_min, v_max].
*/
template<class T> auto Random::next_real(T v_min, T v_max) -> T
{
    CCO_EXPECT_LE(v_min, v_max);
    std::uniform_real_distribution<T> dist(v_min, v_max);
    return dist(m_rng);
}

inline auto random_string(Random &random, Size min_size, Size max_size) -> std::string
{
    return random.next_string(random.next_int(min_size, max_size));
}

inline auto random_string(Random &random, Size max_size) -> std::string
{
    return random_string(random, 0, max_size);
}

inline auto random_binary(Random &random, Size min_size, Size max_size) -> std::string
{
    return random.next_binary(random.next_int(min_size, max_size));
}

inline auto random_binary(Random &random, Size max_size) -> std::string
{
    return random_binary(random, 0, max_size);
}

// TODO: Function for building a random string from a choice of characters.

} // cco

#endif // CCO_TEST_TOOLS_RANDOM_H