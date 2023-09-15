// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_UTILITY_H
#define CALICODB_UTILITY_H

#include <type_traits>

namespace calicodb
{

template <class T>
[[nodiscard]] constexpr auto move(T &&t) noexcept -> typename std::remove_reference_t<T> &&
{
    return static_cast<typename std::remove_reference_t<T> &&>(t);
}

template <class T>
[[nodiscard]] constexpr auto forward(typename std::remove_reference_t<T> &t) noexcept -> T &&
{
    return static_cast<T &&>(t);
}

template <class T>
[[nodiscard]] constexpr auto forward(typename std::remove_reference_t<T> &&t) noexcept -> T &&
{
    static_assert(!std::is_lvalue_reference_v<T>,
                  "forward must not be used to convert an rvalue to an lvalue");
    return static_cast<T &&>(t);
}

template <class T, class U = T>
constexpr auto exchange(T &obj, U &&new_val) -> T
{
    auto old_val = move(obj);
    obj = forward<U>(new_val);
    return old_val;
}

} // namespace calicodb

#endif // CALICODB_UTILITY_H
