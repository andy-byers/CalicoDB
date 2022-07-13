#ifndef CALICO_UTILS_ASSERT_H
#define CALICO_UTILS_ASSERT_H

#include <cstdio>
#include <cstdlib>
#include <limits>
#include <type_traits>

#ifdef NDEBUG
#  define CALICO_EXPECT(cc)
#  define CALICO_VALIDATE(validator)
#else
#  define CALICO_EXPECT_(cc, file, line) utils::impl::handle_expect(cc, #cc, file, line)
#  define CALICO_EXPECT(cc) CALICO_EXPECT_(cc, __FILE__, __LINE__)
#  ifdef CALICO_USE_VALIDATORS
#    define CALICO_VALIDATE(validator) validator
#  else
#    define CALICO_VALIDATE(validator)
#  endif // CALICO_USE_VALIDATORS
#endif // NDEBUG

#define CALICO_EXPECT_TRUE(cc) CALICO_EXPECT(cc)
#define CALICO_EXPECT_FALSE(cc) CALICO_EXPECT_TRUE(!(cc))
#define CALICO_EXPECT_EQ(t1, t2) CALICO_EXPECT((t1) == (t2))
#define CALICO_EXPECT_NE(t1, t2) CALICO_EXPECT((t1) != (t2))
#define CALICO_EXPECT_LT(t1, t2) CALICO_EXPECT((t1) < (t2))
#define CALICO_EXPECT_LE(t1, t2) CALICO_EXPECT((t1) <= (t2))
#define CALICO_EXPECT_GT(t1, t2) CALICO_EXPECT((t1) > (t2))
#define CALICO_EXPECT_GE(t1, t2) CALICO_EXPECT((t1) >= (t2))
#define CALICO_EXPECT_NULL(p) CALICO_EXPECT_TRUE(p == nullptr)
#define CALICO_EXPECT_NOT_NULL(p) CALICO_EXPECT_TRUE((p) != nullptr)
#define CALICO_EXPECT_BOUNDED_BY(Type, t) CALICO_EXPECT_LE(t, std::numeric_limits<Type>::max())
#define CALICO_EXPECT_STATIC(cc, message) static_assert(cc, message)

#define CALICO_TRY(expr) \
    do { \
        if (auto calico_try_result = (expr); !calico_try_result.has_value()) \
            return ErrorResult {calico_try_result.error()}; \
    } while (0)

#define CALICO_TRY_CREATE(name, expr) \
    auto name = (expr); \
    if (!(name).has_value()) { \
        return ErrorResult {(name).error()}; \
    }

#define CCO_TRY(expr) \
    do { \
        if (auto calico_try_result = (expr); !calico_try_result.has_value()) \
            return ErrorResult {calico_try_result.error()}; \
    } while (0)

#define CCO_TRY_ASSIGN(out, expr) \
    do { \
        if (auto calico_try_result = (expr); !calico_try_result.has_value()) {  \
            return ErrorResult {calico_try_result.error()}; \
        } else { \
            (out) = std::move(calico_try_result.value()); \
        } \
    } while (0)

#define CCO_TRY_CREATE(type, out, expr) \
    auto calico_try_##out = (expr); \
    if (!calico_try_##out.has_value()) {  \
        return ErrorResult {calico_try_##out.error()}; \
    } \
    auto out = std::move(*calico_try_##out);

namespace calico::utils::impl {

inline auto handle_expect(bool expectation, const char *repr, const char *file, int line) noexcept
{
    if (!expectation) {
        fprintf(stderr, "Expectation `%s` failed at %s:%d\n", repr, file, line);
        std::abort();
    }
}

} // calico::utils::impl

#endif // CALICO_UTILS_ASSERT_H
