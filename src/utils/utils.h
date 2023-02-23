#ifndef CALICO_UTILS_H
#define CALICO_UTILS_H

#include <cstdio>
#include <cstdlib>
#include <string>
#include "calico/status.h"

#if NDEBUG
#  define CALICO_EXPECT_(expr, file, line)
#else
#  define CALICO_EXPECT_(expr, file, line) Impl::expect(expr, #expr, file, line)
#endif // NDEBUG

#define CALICO_EXPECT_TRUE(expr) CALICO_EXPECT_(expr, __FILE__, __LINE__)
#define CALICO_EXPECT_FALSE(expr) CALICO_EXPECT_TRUE(!(expr))
#define CALICO_EXPECT_EQ(lhs, rhs) CALICO_EXPECT_TRUE((lhs) == (rhs))
#define CALICO_EXPECT_NE(lhs, rhs) CALICO_EXPECT_TRUE((lhs) != (rhs))
#define CALICO_EXPECT_LT(lhs, rhs) CALICO_EXPECT_TRUE((lhs) < (rhs))
#define CALICO_EXPECT_LE(lhs, rhs) CALICO_EXPECT_TRUE((lhs) <= (rhs))
#define CALICO_EXPECT_GT(lhs, rhs) CALICO_EXPECT_TRUE((lhs) > (rhs))
#define CALICO_EXPECT_GE(lhs, rhs) CALICO_EXPECT_TRUE((lhs) >= (rhs))

#define Calico_Try(expr) \
    do { \
        if (auto __calico_try_s = (expr); !__calico_try_s.is_ok()) { \
            return __calico_try_s; \
        } \
    } while (0)

namespace Calico {

namespace Impl {

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
template<class T>
constexpr auto is_power_of_two(T v) noexcept -> bool
{
    return v && !(v & (v - 1));
}

[[nodiscard]]
inline auto get_status_name(const Status &s) noexcept -> const char *
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
    CALICO_EXPECT_TRUE(s.is_ok());
    return "ok";
}

} // namespace Calico

#endif // CALICO_UTILS_H
