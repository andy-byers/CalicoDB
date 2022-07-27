#ifndef CCO_UTILS_ASSERT_H
#define CCO_UTILS_ASSERT_H

#include <cstdio>
#include <cstdlib>
#include <limits>
#include <type_traits>

#ifdef NDEBUG
#  define CCO_EXPECT(cc)
#  define CCO_VALIDATE(validator)
#else
#  define CCO_EXPECT_(cc, file, line) impl::handle_expect(cc, #cc, file, line)
#  define CCO_EXPECT(cc) CCO_EXPECT_(cc, __FILE__, __LINE__)
#endif // NDEBUG

#define CCO_EXPECT_TRUE(cc) CCO_EXPECT(cc)
#define CCO_EXPECT_FALSE(cc) CCO_EXPECT_TRUE(!(cc))
#define CCO_EXPECT_EQ(t1, t2) CCO_EXPECT((t1) == (t2))
#define CCO_EXPECT_NE(t1, t2) CCO_EXPECT((t1) != (t2))
#define CCO_EXPECT_LT(t1, t2) CCO_EXPECT((t1) < (t2))
#define CCO_EXPECT_LE(t1, t2) CCO_EXPECT((t1) <= (t2))
#define CCO_EXPECT_GT(t1, t2) CCO_EXPECT((t1) > (t2))
#define CCO_EXPECT_GE(t1, t2) CCO_EXPECT((t1) >= (t2))
#define CCO_EXPECT_BOUNDED_BY(Type, t) CCO_EXPECT_LE(t, std::numeric_limits<Type>::max())

/**
 * If the expression evaluates to an error object, this macro propagates it up to the caller, otherwise it
 * does nothing.
 *
 * @param expr The expression to evaluate.
 */
#define CCO_TRY(expr) \
    do { \
        if (auto calico_try_result = (expr); !calico_try_result.has_value()) \
            return Err {calico_try_result.error()}; \
    } while (0)

/**
 * If the expression evaluates to an error object, this macro propagates it up to the caller, otherwise it
 * assigns the unwrapped expected value to an existing variable.
 *
 * @param out The new variable.
 * @param expr The expression to evaluate.
 */
#define CCO_TRY_STORE(out, expr) \
    do { \
        if (auto calico_try_result = (expr); !calico_try_result.has_value()) {  \
            return Err {calico_try_result.error()}; \
        } else { \
            (out) = std::move(calico_try_result.value()); \
        } \
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
#define CCO_TRY_CREATE(out, expr) \
    auto calico_try_##out = (expr); \
    if (!calico_try_##out.has_value()) {  \
        return Err {calico_try_##out.error()}; \
    } \
    auto out = std::move(*calico_try_##out)

namespace cco::impl {

inline auto handle_expect(bool expectation, const char *repr, const char *file, int line) noexcept
{
    if (!expectation) {
        fprintf(stderr, "Expectation `%s` failed at %s:%d\n", repr, file, line);
        std::abort();
    }
}

} // cco::impl

#endif // CCO_UTILS_ASSERT_H
