#ifndef CALICO_TEST_TOOLS_RANDOM_H
#define CALICO_TEST_TOOLS_RANDOM_H

#include <algorithm>
#include <random>
#include "calico/options.h"
#include "utils/utils.h"

namespace Calico {

class Random final {
public:
    Random() = default;
    ~Random() = default;

    explicit Random(std::uint32_t seed = 0)
        : m_rng {seed}
    {}

    template<template<class> class Distribution, class T1, class T2>
    auto from(const T1 &lower, const T2 &upper) -> std::common_type_t<T1, T2>
    {
        using Result = std::common_type_t<T1, T2>;
        static_assert(std::is_integral_v<Result> || std::is_floating_point_v<Result>);

        if constexpr (std::is_same_v<Result, Byte>) {
            Distribution<int> distribution {int(lower), int(upper)};
            return distribution(m_rng);
        } else {
            Distribution<Result> distribution {Result(lower), Result(upper)};
            return distribution(m_rng);
        }
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
        static_assert(std::is_integral_v<Result> || std::is_floating_point_v<Result>);

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

} // namespace Calico

#endif // CALICO_TEST_TOOLS_RANDOM_H