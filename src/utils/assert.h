#ifndef CUB_ASSERT_H
#define CUB_ASSERT_H

#include <cstdio>
#include <cstdlib>
#include <limits>
#include <type_traits>

#ifdef CUB_NDEBUG
#  define CUB_EXPECT(cc)
#else
#  define CUB_EXPECT_(cc, file, line) impl::handle_expect(cc, #cc, file, line)
#  define CUB_EXPECT(cc) CUB_EXPECT_(cc, __FILE__, __LINE__)
#endif
#define CUB_EXPECT_TRUE(cc) CUB_EXPECT(cc)
#define CUB_EXPECT_FALSE(cc) CUB_EXPECT_TRUE(!(cc))
#define CUB_EXPECT_EQ(t1, t2) CUB_EXPECT(t1 == t2)
#define CUB_EXPECT_NE(t1, t2) CUB_EXPECT(t1 != t2)
#define CUB_EXPECT_LT(t1, t2) CUB_EXPECT(t1 < t2)
#define CUB_EXPECT_LE(t1, t2) CUB_EXPECT(t1 <= t2)
#define CUB_EXPECT_GT(t1, t2) CUB_EXPECT(t1 > t2)
#define CUB_EXPECT_GE(t1, t2) CUB_EXPECT(t1 >= t2)
#define CUB_EXPECT_NULL(p) CUB_EXPECT_TRUE(p == nullptr)
#define CUB_EXPECT_NOT_NULL(p) CUB_EXPECT_TRUE(p != nullptr)
#define CUB_EXPECT_BOUNDED_BY(Type, t) CUB_EXPECT_LE(t, std::numeric_limits<Type>::max())
#define CUB_EXPECT_STATIC(cc, message) static_assert(cc, message)

namespace cub::impl {

inline auto handle_expect(bool expectation, const char *repr, const char *file, int line) noexcept
{
    if (!expectation) {
        fprintf(stderr, "Expectation `%s` failed at %s:%d\n", repr, file, line);
        std::abort();
    }
}

} // cub

#endif // CUB_ASSERT_H
