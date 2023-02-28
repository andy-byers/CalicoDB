#ifndef CALICODB_UTILS_H
#define CALICODB_UTILS_H

#include "calicodb/status.h"
#include <cstdio>
#include <cstdlib>

#if NDEBUG
#define CDB_EXPECT_(expr, file, line)
#else
#define CDB_EXPECT_(expr, file, line) Impl::expect(expr, #expr, file, line)
#endif // NDEBUG

#define CDB_EXPECT_TRUE(expr) CDB_EXPECT_(expr, __FILE__, __LINE__)
#define CDB_EXPECT_FALSE(expr) CDB_EXPECT_TRUE(!(expr))
#define CDB_EXPECT_EQ(lhs, rhs) CDB_EXPECT_TRUE((lhs) == (rhs))
#define CDB_EXPECT_NE(lhs, rhs) CDB_EXPECT_TRUE((lhs) != (rhs))
#define CDB_EXPECT_LT(lhs, rhs) CDB_EXPECT_TRUE((lhs) < (rhs))
#define CDB_EXPECT_LE(lhs, rhs) CDB_EXPECT_TRUE((lhs) <= (rhs))
#define CDB_EXPECT_GT(lhs, rhs) CDB_EXPECT_TRUE((lhs) > (rhs))
#define CDB_EXPECT_GE(lhs, rhs) CDB_EXPECT_TRUE((lhs) >= (rhs))

#define CDB_TRY(expr)                                             \
    do {                                                             \
        if (auto __calico_try_s = (expr); !__calico_try_s.is_ok()) { \
            return __calico_try_s;                                   \
        }                                                            \
    } while (0)

namespace calicodb
{

namespace Impl
{

inline constexpr auto expect(bool cond, const char *repr, const char *file, int line) noexcept -> void
{
    if (!cond) {
        std::fprintf(stderr, "expectation (%s) failed at %s:%d\n", repr, file, line);
        std::abort();
    }
}

} // namespace Impl

static constexpr Size MINIMUM_PAGE_SIZE {0x200};
static constexpr Size MAXIMUM_PAGE_SIZE {0x8000};

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

} // namespace calicodb

#endif // CALICODB_UTILS_H
