#ifndef CCO_UTILS_ASSERT_H
#define CCO_UTILS_ASSERT_H

#include "calico/status.h"
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <type_traits>

#ifdef NDEBUG
#  define CCO_EXPECT_BOOL_(cc)
#  define CCO_EXPECT_STAT_(s)
#else
#  define CCO_EXPECT_BOOL_(cc, file, line) impl::handle_expect(cc, #cc, file, line)
#  define CCO_EXPECT_STAT_(s, file, line) impl::handle_expect(s, file, line)
#endif // NDEBUG

#define CCO_EXPECT_OK(s) CCO_EXPECT_STAT_(s, __FILE__, __LINE__)
#define CCO_EXPECT_TRUE(cc) CCO_EXPECT_BOOL_(cc, __FILE__, __LINE__)
#define CCO_EXPECT_FALSE(cc) CCO_EXPECT_TRUE(!(cc))
#define CCO_EXPECT_EQ(t1, t2) CCO_EXPECT_TRUE((t1) == (t2))
#define CCO_EXPECT_NE(t1, t2) CCO_EXPECT_TRUE((t1) != (t2))
#define CCO_EXPECT_LT(t1, t2) CCO_EXPECT_TRUE((t1) < (t2))
#define CCO_EXPECT_LE(t1, t2) CCO_EXPECT_TRUE((t1) <= (t2))
#define CCO_EXPECT_GT(t1, t2) CCO_EXPECT_TRUE((t1) > (t2))
#define CCO_EXPECT_GE(t1, t2) CCO_EXPECT_TRUE((t1) >= (t2))
#define CCO_EXPECT_BOUNDED_BY(Type, t) CCO_EXPECT_LE(t, std::numeric_limits<Type>::max())

/**
 * If the expression evaluates to an error object, this macro propagates it up to the caller, otherwise it
 * does nothing.
 *
 * @param expr The expression to evaluate.
 */
#define CCO_TRY(expr)                                                        \
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
#define CCO_TRY_STORE(out, expr)                                               \
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
#define CCO_TRY_CREATE(out, expr)              \
    auto calico_try_##out = (expr);            \
    if (!calico_try_##out.has_value()) {       \
        return Err {calico_try_##out.error()}; \
    }                                          \
    auto out = std::move(calico_try_##out.value())

namespace cco::impl {

inline constexpr auto handle_expect(bool expectation, const char *repr, const char *file, int line) noexcept -> void
{
    if (!expectation) {
        fprintf(stderr, "Expectation `%s` failed at %s:%d\n", repr, file, line);
        std::abort();
    }
}

inline auto handle_expect(const Status &expectation, const char *file, int line) noexcept -> void
{
    static constexpr auto format = "Unexpected \"%s\" status at %s:%d: ";

    if (expectation.is_ok()) {
        return;
    } else if (expectation.is_not_found()) {
        fprintf(stderr, format, "NOT_FOUND", file, line);
    } else if (expectation.is_system_error()) {
        fprintf(stderr, format, "SYSTEM_ERROR", file, line);
    } else if (expectation.is_logic_error()) {
        fprintf(stderr, format, "LOGIC_ERROR", file, line);
    } else if (expectation.is_corruption()) {
        fprintf(stderr, format, "CORRUPTION", file, line);
    } else if (expectation.is_invalid_argument()) {
        fprintf(stderr, format, "INVALID_ARGUMENT", file, line);
    } else {
        fprintf(stderr, format, "UNKNOWN", file, line);
    }
    puts(expectation.what().c_str());
    std::abort();
}

} // namespace cco::impl

#endif // CCO_UTILS_ASSERT_H
