// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_UTILS_H
#define CALICODB_UTILS_H

#include "calicodb/options.h"
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

#define CALICODB_EXPECT_TRUE(expr) assert(expr)
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

#define ARRAY_SIZE(x) (sizeof(x) / sizeof((x)[0]))

namespace calicodb
{

[[nodiscard]] inline auto is_aligned(void *ptr, size_t alignment) -> bool
{
    CALICODB_EXPECT_NE(alignment, 0);
    return reinterpret_cast<std::uintptr_t>(ptr) % alignment == 0;
}

template <class Callback>
auto busy_wait(BusyHandler *handler, const Callback &callback) -> Status
{
    for (unsigned n = 0;; ++n) {
        auto s = callback();
        if (s.is_busy()) {
            if (handler && handler->exec(n)) {
                continue;
            }
        }
        return s;
    }
}

static constexpr size_t kMinFrameCount = 1;
static constexpr size_t kMaxCacheSize = 1 << 30;
static constexpr size_t kTreeBufferLen = 3 * kPageSize;
static constexpr auto kDefaultWalSuffix = "-wal";
static constexpr auto kDefaultShmSuffix = "-shm";

// Additional file locking modes that cannot be requested directly
enum { kLockUnlocked = 0 };

struct Id {
    static constexpr uint32_t kNull = 0;
    static constexpr uint32_t kRoot = 1;

    struct Hash {
        auto operator()(const Id &id) const -> uint64_t
        {
            return id.value;
        }
    };

    Id() = default;

    template <class T>
    explicit constexpr Id(T t)
        : value(static_cast<uint32_t>(t))
    {
    }

    [[nodiscard]] static constexpr auto from_index(size_t index) -> Id
    {
        return Id(index + 1);
    }

    [[nodiscard]] static constexpr auto null() -> Id
    {
        return Id(kNull);
    }

    [[nodiscard]] static constexpr auto root() -> Id
    {
        return Id(kRoot);
    }

    [[nodiscard]] constexpr auto is_null() const -> bool
    {
        return value == kNull;
    }

    [[nodiscard]] constexpr auto is_root() const -> bool
    {
        return value == kRoot;
    }

    [[nodiscard]] constexpr auto as_index() const -> size_t
    {
        CALICODB_EXPECT_NE(value, null().value);
        return value - 1;
    }

    uint32_t value = kNull;
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
auto operator<<(std::ostream &os, const Slice &slice) -> std::ostream &
{
    return os << slice.to_string();
}

} // namespace calicodb

#endif // CALICODB_UTILS_H
