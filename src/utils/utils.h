//
// Created by Andrew Byers on 5/18/22.
//

#ifndef CUB_UTILS_UTILS_H
#define CUB_UTILS_UTILS_H

#include <string>
#include "assert.h"
#include "common.h"

namespace cub {

// Source: http://graphics.stanford.edu/~seander/bithacks.html#DetermineIfPowerOf2
template<class T> auto is_power_of_two(T v) noexcept -> bool
{
    return v && !(v & (v-1));
}

inline auto compose_wal_path(const std::string &db_path) -> std::string
{
    return db_path + ".wal";
}

} // db

#endif // CUB_UTILS_UTILS_H
