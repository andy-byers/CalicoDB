#ifndef CALICO_TEST_TOOLS_RANDOM_H
#define CALICO_TEST_TOOLS_RANDOM_H

#include <algorithm>
#include <random>
#include "calico/options.h"
#include "utils/expect.h"

namespace calico {

template<class T>
static constexpr bool IsPrimitive = std::is_integral_v<T> || std::is_floating_point_v<T>;

class Random_ final {
public:
    ~Random_() = default;

    explicit Random_(std::uint32_t seed = 0)
        : m_rng {seed}
    {}

    template<template <class> class Distribution, class T1, class T2>
    auto from(const T1 &lower, const T2 &upper) -> std::common_type_t<T1, T2>
    {
        using Result = std::common_type_t<T1, T2>;
        static_assert(IsPrimitive<Result>);

        Distribution<Result> distribution {Result(lower), Result(upper)};
        return distribution(m_rng);
    }

    template<class Container, class T1, class T2>
    auto get(const T1 &lower, const T2 &upper, Size n) -> Container
    {
        Container container;
        std::generate_n(back_inserter(container), n, [this, &lower, &upper] {
            return get(lower, upper);
        });
        return container;
    }

    template<class T1, class T2>
    auto get(const T1 &lower, const T2 &upper) -> std::common_type_t<T1, T2>
    {
        using Result = std::common_type_t<T1, T2>;
        static_assert(IsPrimitive<Result>);

        if constexpr (std::is_integral_v<Result>) {
            return from<std::uniform_int_distribution>(lower, upper);
        } else {
            return from<std::uniform_real_distribution>(lower, upper);
        }
    }

    template<class T>
    auto get(const T &upper) -> T
    {
        return get(T {}, upper);
    }

    template<class T>
    auto get() -> T
    {
        return get(std::numeric_limits<T>::min(),
                   std::numeric_limits<T>::max());
    }

    auto rng() -> std::default_random_engine&
    {
        return m_rng;
    }

protected:
    std::default_random_engine m_rng;
};

class Random final {
public:
    using Seed = std::uint32_t;

    ~Random() = default;

    explicit Random(Seed seed = 0)
        : m_rng {seed}
    {}

    auto next_string(Size) -> std::string;
    auto next_binary(Size) -> std::string;
    template<class C> auto shuffle(C&) -> void;

    // NOTE: All upper bounds are inclusive.
    template<class T> auto next_int(T) -> T;
    template<class T1, class T2> auto next_int(T1, T2) -> T1;
    template<class T> auto next_real(T) -> T;
    template<class T1, class T2> auto next_real(T1, T2) -> T1;

protected:
    std::default_random_engine m_rng;
};

template<class Container> auto Random::shuffle(Container &data) -> void
{
    std::shuffle(begin(data), end(data), m_rng);
}

template<class T>
auto Random::next_int(T v_max) -> T
{
    return next_int(T {}, v_max);
}

template<class T1, class T2>
auto Random::next_int(T1 v_min, T2 v_max) -> T1
{
    CALICO_EXPECT_LE(v_min, v_max);
    std::uniform_int_distribution<T1> dist(v_min, T1(v_max));
    return dist(m_rng);
}

template<class T>
auto Random::next_real(T v_max) -> T
{
    return next_real(T {}, v_max);
}

template<class T1, class T2>
auto Random::next_real(T1 v_min, T2 v_max) -> T1
{
    CALICO_EXPECT_LE(v_min, v_max);
    std::uniform_real_distribution<T1> dist(v_min, T1(v_max));
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

#endif // CALICO_TEST_TOOLS_RANDOM_H