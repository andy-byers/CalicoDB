// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_UTILS_H
#define CALICODB_UTILS_H

#include "calicodb/status.h"
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <ostream>

#if NDEBUG
#define CALICODB_EXPECT_(expr, file, line)
#else
#define CALICODB_EXPECT_(expr, file, line) calicodb::expect_impl(expr, #expr, file, line)
#ifdef CALICODB_TEST
#define CALICODB_EXPENSIVE_CHECKS
#endif // CALICODB_TEST
#endif // !NDEBUG

#define CALICODB_EXPECT_TRUE(expr) CALICODB_EXPECT_(expr, __FILE__, __LINE__)
#define CALICODB_EXPECT_FALSE(expr) CALICODB_EXPECT_TRUE(!(expr))
#define CALICODB_EXPECT_EQ(lhs, rhs) CALICODB_EXPECT_TRUE((lhs) == (rhs))
#define CALICODB_EXPECT_NE(lhs, rhs) CALICODB_EXPECT_TRUE((lhs) != (rhs))
#define CALICODB_EXPECT_LT(lhs, rhs) CALICODB_EXPECT_TRUE((lhs) < (rhs))
#define CALICODB_EXPECT_LE(lhs, rhs) CALICODB_EXPECT_TRUE((lhs) <= (rhs))
#define CALICODB_EXPECT_GT(lhs, rhs) CALICODB_EXPECT_TRUE((lhs) > (rhs))
#define CALICODB_EXPECT_GE(lhs, rhs) CALICODB_EXPECT_TRUE((lhs) >= (rhs))

#define CALICODB_TRY(expr)                                               \
    do {                                                                 \
        if (auto __calicodb_try_s = (expr); !__calicodb_try_s.is_ok()) { \
            return __calicodb_try_s;                                     \
        }                                                                \
    } while (0)

#define ARRAY_SIZE(x) (sizeof(x) / sizeof(x[0]))

namespace calicodb
{

inline constexpr auto expect_impl(bool cond, const char *repr, const char *file, int line) noexcept -> void
{
    if (!cond) {
        std::fprintf(stderr, "expectation (%s) failed at %s:%d\n", repr, file, line);
        std::abort();
    }
}

static constexpr std::size_t kMinFrameCount = 8;
static constexpr std::size_t kMaxCacheSize = 1 << 30;
static constexpr auto kDefaultWalSuffix = "-wal";
static constexpr auto kDefaultShmSuffix = "-shm";
static constexpr auto kDefaultLogSuffix = "-log";

// Fixed-width unsigned integers for use in the database file format
using U8 = std::uint8_t;
using U16 = std::uint16_t;
using U32 = std::uint32_t;
using U64 = std::uint64_t;

// Additional file locking modes that cannot be requested directly
enum : int { kLockUnlocked = 0 };

struct Id {
    static constexpr U32 kNull = 0;
    static constexpr U32 kRoot = 1;
    static constexpr auto kSize = sizeof(kNull);

    struct Hash {
        auto operator()(const Id &id) const -> U64
        {
            return id.value;
        }
    };

    Id() = default;

    template <class T>
    explicit constexpr Id(T t)
        : value(static_cast<U32>(t))
    {
    }

    [[nodiscard]] static constexpr auto from_index(std::size_t index) noexcept -> Id
    {
        return Id(index + 1);
    }

    [[nodiscard]] static constexpr auto null() noexcept -> Id
    {
        return Id(kNull);
    }

    [[nodiscard]] static constexpr auto root() noexcept -> Id
    {
        return Id(kRoot);
    }

    [[nodiscard]] constexpr auto is_null() const noexcept -> bool
    {
        return value == kNull;
    }

    [[nodiscard]] constexpr auto is_root() const noexcept -> bool
    {
        return value == kRoot;
    }

    [[nodiscard]] constexpr auto as_index() const noexcept -> std::size_t
    {
        CALICODB_EXPECT_NE(value, null().value);
        return value - 1;
    }

    U32 value = kNull;
};

template <class T>
auto operator<<(std::ostream &os, Id id) -> std::ostream &
{
    return os << id.value;
}

inline auto operator<(Id lhs, Id rhs) -> bool
{
    return lhs.value < rhs.value;
}

inline auto operator<=(Id lhs, Id rhs) -> bool
{
    return lhs.value <= rhs.value;
}

inline auto operator==(Id lhs, Id rhs) -> bool
{
    return lhs.value == rhs.value;
}

inline auto operator!=(Id lhs, Id rhs) -> bool
{
    return lhs.value != rhs.value;
}

template <class T>
auto operator<<(std::ostream &os, Id id) -> std::ostream &
{
    return os << id.value;
}

template <class T>
auto operator<<(std::ostream &os, const Slice &slice) -> std::ostream &
{
    return os << slice.to_string();
}

} // namespace calicodb

#endif // CALICODB_UTILS_H
