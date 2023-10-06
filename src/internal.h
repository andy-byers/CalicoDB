// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_INTERNAL_H
#define CALICODB_INTERNAL_H

#include "calicodb/env.h"
#include "calicodb/options.h"
#include "utility.h"
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>

#ifdef NDEBUG
#define CALICODB_DEBUG_DELAY(env)
#else
#define CALICODB_DEBUG_DELAY(env) debug_delay_impl(env)
#endif

#define CALICODB_EXPECT_TRUE(expr) assert(expr)
#define CALICODB_EXPECT_FALSE(expr) CALICODB_EXPECT_TRUE(!(expr))
#define CALICODB_EXPECT_EQ(lhs, rhs) CALICODB_EXPECT_TRUE((lhs) == (rhs))
#define CALICODB_EXPECT_NE(lhs, rhs) CALICODB_EXPECT_TRUE((lhs) != (rhs))
#define CALICODB_EXPECT_LT(lhs, rhs) CALICODB_EXPECT_TRUE((lhs) < (rhs))
#define CALICODB_EXPECT_LE(lhs, rhs) CALICODB_EXPECT_TRUE((lhs) <= (rhs))
#define CALICODB_EXPECT_GT(lhs, rhs) CALICODB_EXPECT_TRUE((lhs) > (rhs))
#define CALICODB_EXPECT_GE(lhs, rhs) CALICODB_EXPECT_TRUE((lhs) >= (rhs))
#define CALICODB_DEBUG_TRAP assert(false && __FUNCTION__)

#define ARRAY_SIZE(x) (sizeof(x) / sizeof((x)[0]))

namespace calicodb
{

// Possibly cause the calling thread to sleep for a random length of time
// Used to provoke timing problems in code that uses atomics.
inline auto debug_delay_impl(Env &env) -> void
{
    if (env.rand() % 250 == 0) {
        env.sleep(env.rand() % 1'000);
    }
}

[[nodiscard]] inline auto is_aligned(const void *ptr, size_t alignment) -> bool
{
    CALICODB_EXPECT_GT(alignment, 1);
    CALICODB_EXPECT_EQ(alignment & (alignment - 1), 0);
    return 0 == (reinterpret_cast<uintptr_t>(ptr) & (alignment - 1));
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

// Limit on the size of a single allocation
// This is, consequently, the maximum size of a record key or value.
static constexpr uint32_t kMaxAllocation = 2'000'000'000;

// Bounds on the size of a database page
static constexpr uint32_t kMinPageSize = 512;
static constexpr uint32_t kMaxPageSize = 32'768;

// Bounds on the size of the page cache
static constexpr size_t kMinFrameCount = 1;
static constexpr size_t kMaxCacheSize = 1 << 30;

// Number of scratch pages needed to perform tree operations
static constexpr size_t kScratchBufferPages = 2;

// Page number of the first pointer map page
static constexpr size_t kFirstMapPage = 2;

// Default filename suffixes for the WAL and shm files
static constexpr Slice kDefaultWalSuffix = "-wal";
static constexpr Slice kDefaultShmSuffix = "-shm";

// Additional file locking modes that cannot be requested directly
enum { kLockUnlocked = 0 };

struct Id {
    static constexpr uint32_t kNull = 0;
    static constexpr uint32_t kRoot = 1;

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
        CALICODB_EXPECT_NE(value, kNull);
        return value - 1;
    }

    uint32_t value = kNull;
};

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
constexpr auto minval(T t1, T t2) -> T
{
    return t1 < t2 ? t1 : t2;
}

template <class T>
constexpr auto maxval(T t1, T t2) -> T
{
    return t1 > t2 ? t1 : t2;
}

} // namespace calicodb

#endif // CALICODB_INTERNAL_H
