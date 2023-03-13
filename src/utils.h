// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_UTILS_H
#define CALICODB_UTILS_H

#include "calicodb/status.h"
#include <cstdint>
#include <cstdio>
#include <cstdlib>

#if NDEBUG
#define CDB_EXPECT_(expr, file, line)
#else
#define CDB_EXPECT_(expr, file, line) impl::expect(expr, #expr, file, line)
#endif // NDEBUG

#define CDB_EXPECT_TRUE(expr) CDB_EXPECT_(expr, __FILE__, __LINE__)
#define CDB_EXPECT_FALSE(expr) CDB_EXPECT_TRUE(!(expr))
#define CDB_EXPECT_EQ(lhs, rhs) CDB_EXPECT_TRUE((lhs) == (rhs))
#define CDB_EXPECT_NE(lhs, rhs) CDB_EXPECT_TRUE((lhs) != (rhs))
#define CDB_EXPECT_LT(lhs, rhs) CDB_EXPECT_TRUE((lhs) < (rhs))
#define CDB_EXPECT_LE(lhs, rhs) CDB_EXPECT_TRUE((lhs) <= (rhs))
#define CDB_EXPECT_GT(lhs, rhs) CDB_EXPECT_TRUE((lhs) > (rhs))
#define CDB_EXPECT_GE(lhs, rhs) CDB_EXPECT_TRUE((lhs) >= (rhs))

#define CDB_TRY(expr)                                          \
    do {                                                       \
        if (auto __cdb_try_s = (expr); !__cdb_try_s.is_ok()) { \
            return __cdb_try_s;                                \
        }                                                      \
    } while (0)

namespace calicodb
{

namespace impl
{

inline constexpr auto expect(bool cond, const char *repr, const char *file, int line) noexcept -> void
{
    if (!cond) {
        std::fprintf(stderr, "expectation (%s) failed at %s:%d\n", repr, file, line);
        std::abort();
    }
}

} // namespace impl

static constexpr std::size_t kMinPageSize {0x200};
static constexpr std::size_t kMaxPageSize {0x8000};
static constexpr auto kDefaultWalSuffix = "-wal-";
static constexpr auto kDefaultLogSuffix = "-log";

// Source: http://graphics.stanford.edu/~seander/bithacks.html#DetermineIfPowerOf2
template <class T>
constexpr auto is_power_of_two(T v) noexcept -> bool
{
    return v && !(v & (v - 1));
}

[[nodiscard]] inline auto get_status_name(const Status &s) noexcept -> const char *
{
    if (s.is_not_found()) {
        return "not found";
    } else if (s.is_system_error()) {
        return "system error";
    } else if (s.is_logic_error()) {
        return "logic error";
    } else if (s.is_corruption()) {
        return "corruption";
    } else if (s.is_invalid_argument()) {
        return "invalid argument";
    }
    CDB_EXPECT_TRUE(s.is_ok());
    return "ok";
}

struct Id {
    static constexpr std::uint64_t kNull {0};
    static constexpr std::uint64_t kRoot {1};

    struct Hash {
        auto operator()(const Id &id) const -> std::uint64_t
        {
            return id.value;
        }
    };

    [[nodiscard]] static constexpr auto from_index(std::uint64_t index) noexcept -> Id
    {
        return {index + 1};
    }

    [[nodiscard]] static constexpr auto null() noexcept -> Id
    {
        return {kNull};
    }

    [[nodiscard]] static constexpr auto root() noexcept -> Id
    {
        return {kRoot};
    }

    [[nodiscard]] constexpr auto is_null() const noexcept -> bool
    {
        return value == kNull;
    }

    [[nodiscard]] constexpr auto is_root() const noexcept -> bool
    {
        return value == kRoot;
    }

    [[nodiscard]] constexpr auto as_index() const noexcept -> std::uint64_t
    {
        CDB_EXPECT_NE(value, null().value);
        return value - 1;
    }

    std::uint64_t value {};
};

inline auto operator<(Id lhs, Id rhs) -> bool
{
    return lhs.value < rhs.value;
}

inline auto operator>(Id lhs, Id rhs) -> bool
{
    return lhs.value > rhs.value;
}

inline auto operator<=(Id lhs, Id rhs) -> bool
{
    return lhs.value <= rhs.value;
}

inline auto operator>=(Id lhs, Id rhs) -> bool
{
    return lhs.value >= rhs.value;
}

inline auto operator==(Id lhs, Id rhs) -> bool
{
    return lhs.value == rhs.value;
}

inline auto operator!=(Id lhs, Id rhs) -> bool
{
    return lhs.value != rhs.value;
}

using Lsn = Id;

} // namespace calicodb

#endif // CALICODB_UTILS_H
