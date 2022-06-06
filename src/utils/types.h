/**
 * References
 * (1) https://en.cppreference.com/w/cpp/utility/exchange
 */

#ifndef CUB_UTILS_TYPES_H
#define CUB_UTILS_TYPES_H

#include "cub/common.h"

namespace cub {

template<class Value> struct Unique {

    template<class V> explicit Unique(V v)
        : value{std::move(v)} {}

    Unique(const Unique &) = delete;
    auto operator=(const Unique &) -> Unique & = delete;

    Unique(Unique &&rhs) noexcept
    {
        *this = std::move(rhs);
    }

    auto operator=(Unique &&rhs) noexcept -> Unique &
    {
        // TODO: std::exchange() is not noexcept until C++23, but (1) doesn't specify
        //       any exceptions it could throw. Depends on `Value`?
        value = std::exchange(rhs.value, {});
        return *this;
    }

    Value value;
};

} // cub

#endif // CUB_UTILS_TYPES_H
