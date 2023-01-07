#ifndef CALICO_UTILS_EXPECT_H
#define CALICO_UTILS_EXPECT_H

#include "calico/status.h"
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <type_traits>

#ifdef NDEBUG
#  define CALICO_EXPECT_(cc, file, line)
#else
#  define CALICO_EXPECT_(cc, file, line) Impl::handle_expect(cc, #cc, file, line)
#endif // NDEBUG

#define CALICO_EXPECT_TRUE(cc) CALICO_EXPECT_(cc, __FILE__, __LINE__)
#define CALICO_EXPECT_FALSE(cc) CALICO_EXPECT_TRUE(!(cc))
#define CALICO_EXPECT_EQ(t1, t2) CALICO_EXPECT_TRUE((t1) == (t2))
#define CALICO_EXPECT_NE(t1, t2) CALICO_EXPECT_TRUE((t1) != (t2))
#define CALICO_EXPECT_LT(t1, t2) CALICO_EXPECT_TRUE((t1) < (t2))
#define CALICO_EXPECT_LE(t1, t2) CALICO_EXPECT_TRUE((t1) <= (t2))
#define CALICO_EXPECT_GT(t1, t2) CALICO_EXPECT_TRUE((t1) > (t2))
#define CALICO_EXPECT_GE(t1, t2) CALICO_EXPECT_TRUE((t1) >= (t2))

#define CALICO_TRY_S(expr) \
    do { \
        if (auto calico_try_status = (expr); !calico_try_status.is_ok()) \
            return calico_try_status; \
    } while (0)

#define CALICO_TRY_R(expr) \
    do { \
        if (auto calico_try_result = (expr); !calico_try_result.has_value()) \
            return tl::make_unexpected(calico_try_result.error()); \
    } while (0)

#define CALICO_PUT_R(out, expr) \
    do { \
        if (auto calico_try_result = (expr); !calico_try_result.has_value()) { \
            return tl::make_unexpected(calico_try_result.error()); \
        } else { \
            (out) = std::move(calico_try_result.value()); \
        } \
    } while (0)

#define CALICO_NEW_R(out, expr) \
    auto calico_try_##out = (expr); \
    if (!calico_try_##out.has_value()) { \
        return tl::make_unexpected(calico_try_##out.error()); \
    } \
    auto out = std::move(*calico_try_##out)

namespace Calico::Impl {

inline constexpr auto handle_expect(bool expectation, const char *repr, const char *file, int line) noexcept -> void
{
    if (!expectation) {
        fprintf(stderr, "expectation `%s` failed at %s:%d\n", repr, file, line);
        std::abort();
    }
}

} // namespace Calico::impl

#endif // CALICO_UTILS_EXPECT_H
