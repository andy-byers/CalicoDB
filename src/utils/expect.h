#ifndef CALICO_UTILS_ASSERT_H
#define CALICO_UTILS_ASSERT_H

#include "calico/status.h"
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <type_traits>

#ifdef NDEBUG
#  define CALICO_EXPECT_(cc, file, line)
#else
#  define CALICO_EXPECT_(cc, file, line) impl::handle_expect(cc, #cc, file, line)
#endif // NDEBUG

#define CALICO_EXPECT_TRUE(cc) CALICO_EXPECT_(cc, __FILE__, __LINE__)
#define CALICO_EXPECT_FALSE(cc) CALICO_EXPECT_TRUE(!(cc))
#define CALICO_EXPECT_EQ(t1, t2) CALICO_EXPECT_TRUE((t1) == (t2))
#define CALICO_EXPECT_NE(t1, t2) CALICO_EXPECT_TRUE((t1) != (t2))
#define CALICO_EXPECT_LT(t1, t2) CALICO_EXPECT_TRUE((t1) < (t2))
#define CALICO_EXPECT_LE(t1, t2) CALICO_EXPECT_TRUE((t1) <= (t2))
#define CALICO_EXPECT_GT(t1, t2) CALICO_EXPECT_TRUE((t1) > (t2))
#define CALICO_EXPECT_GE(t1, t2) CALICO_EXPECT_TRUE((t1) >= (t2))
#define CALICO_EXPECT_BOUNDED_BY(Type, t) CALICO_EXPECT_LE(t, std::numeric_limits<Type>::max())

/**
 * If the expression evaluates to an error object, this macro propagates it up to the caller, otherwise it
 * does nothing.
 *
 * @param expr The expression to evaluate.
 */
#define CALICO_TRY(expr)                                                        \
    do {                                                                     \
        if (auto calico_try_result = (expr); !calico_try_result.has_value()) \
            return Err {calico_try_result.error()};                          \
    } while (0)

/**
 * If the expression evaluates to an error object, this macro propagates it up to the caller, otherwise it
 * assigns the unwrapped expected value to an existing variable.
 *
 * @param out The new variable.
 * @param expr The expression to evaluate.
 */
#define CALICO_TRY_STORE(out, expr)                                               \
    do {                                                                       \
        if (auto calico_try_result = (expr); !calico_try_result.has_value()) { \
            return Err {calico_try_result.error()};                            \
        } else {                                                               \
            (out) = std::move(calico_try_result.value());                      \
        }                                                                      \
    } while (0)

/**
 * If the expression evaluates to an error object, this macro propagates it up to the caller, otherwise it
 * assigns the unwrapped expected value to a new variable.
 *
 * This macro needs to be used with care as it evaluates to multiple statements.
 *
 * @param out A valid identifier denoting the name of the new variable.
 * @param expr The expression to evaluate.
 */
#define CALICO_TRY_CREATE(out, expr)              \
    auto calico_try_##out = (expr);            \
    if (!calico_try_##out.has_value()) {       \
        return Err {calico_try_##out.error()}; \
    }                                          \
    auto out = std::move(*calico_try_##out)

namespace calico::impl {

inline constexpr auto handle_expect(bool expectation, const char *repr, const char *file, int line) noexcept -> void
{
    if (!expectation) {
        fprintf(stderr, "expectation `%s` failed at %s:%d\n", repr, file, line);
        std::abort();
    }
}

} // namespace calico::impl

#endif // CALICO_UTILS_ASSERT_H
