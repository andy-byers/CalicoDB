#ifndef CALICO_UTILS_H
#define CALICO_UTILS_H

#include "calico/status.h"
#include <spdlog/fmt/fmt.h>

#if NDEBUG
#  define CALICO_EXPECT_(expr, file, line)
#else
#  define CALICO_EXPECT_(expr, file, line) Impl::handle_expect(expr, #expr, file, line)
#endif // NDEBUG

#define CALICO_EXPECT_TRUE(expr) CALICO_EXPECT_(expr, __FILE__, __LINE__)
#define CALICO_EXPECT_FALSE(expr) CALICO_EXPECT_TRUE(!(expr))
#define CALICO_EXPECT_EQ(lhs, rhs) CALICO_EXPECT_TRUE((lhs) == (rhs))
#define CALICO_EXPECT_NE(lhs, rhs) CALICO_EXPECT_TRUE((lhs) != (rhs))
#define CALICO_EXPECT_LT(lhs, rhs) CALICO_EXPECT_TRUE((lhs) < (rhs))
#define CALICO_EXPECT_LE(lhs, rhs) CALICO_EXPECT_TRUE((lhs) <= (rhs))
#define CALICO_EXPECT_GT(lhs, rhs) CALICO_EXPECT_TRUE((lhs) > (rhs))
#define CALICO_EXPECT_GE(lhs, rhs) CALICO_EXPECT_TRUE((lhs) >= (rhs))

#define Calico_Try_S(expr) \
    do { \
        if (auto __try_s = (expr); !__try_s.is_ok()) { \
            return __try_s; \
        } \
    } while (0)

#define Calico_Try_R(expr) \
    do { \
        if (auto __try_r = (expr); !__try_r.has_value()) { \
            return tl::make_unexpected(__try_r.error()); \
        } \
    } while (0)

// "out" must be an existing identifier.
#define Calico_Put_R(out, expr) \
    do { \
        if (auto __put_r = (expr); !__put_r.has_value()) { \
            return tl::make_unexpected(__put_r.error()); \
        } else { \
            (out) = std::move(__put_r.value()); \
        } \
    } while (0)

// "out" must be a nonexistent identifier.
#define Calico_New_R(out, expr) \
    auto __new_##out = (expr); \
    if (!__new_##out.has_value()) { \
        return tl::make_unexpected(__new_##out.error()); \
    } \
    auto out = std::move(*__new_##out)

namespace Calico {

namespace Impl {

    inline constexpr auto handle_expect(bool expectation, const char *repr, const char *file, int line) noexcept -> void
    {
        if (!expectation) {
            fprintf(stderr, "expectation (%s) failed at %s:%d\n", repr, file, line);
            std::abort();
        }
    }

} // namespace impl

static constexpr Size MINIMUM_PAGE_SIZE {0x100};
static constexpr Size MAXIMUM_PAGE_SIZE {0x8000};
static constexpr Size MINIMUM_LOG_MAX_SIZE {0xA000};
static constexpr Size MAXIMUM_LOG_MAX_SIZE {0xA00000};
static constexpr Size MINIMUM_LOG_MAX_FILES {1};
static constexpr Size MAXIMUM_LOG_MAX_FILES {32};

// Source: http://graphics.stanford.edu/~seander/bithacks.html#DetermineIfPowerOf2
template<class T>
constexpr auto is_power_of_two(T v) noexcept -> bool
{
    return v && !(v & (v - 1));
}

[[nodiscard]]
inline auto get_status_name(const Status &s) noexcept -> std::string
{
    if (s.is_ok()) {
        return "OK";
    } else if (s.is_not_found()) {
        return "not found";
    } else if (s.is_system_error()) {
        return "system error";
    } else if (s.is_logic_error()) {
        return "logic error";
    } else if (s.is_corruption()) {
        return "corruption";
    } else if (s.is_invalid_argument()) {
        return "invalid argument";
    } else {
        return "unknown";
    }
}

inline auto ok() -> Status
{
    return Status::ok();
}

template<class ...Ts>
[[nodiscard]]
auto invalid_argument(const std::string_view &format, Ts &&...ts) -> Status
{
    return Status::invalid_argument(fmt::format(format, std::forward<Ts>(ts)...));
}

template<class ...Ts>
[[nodiscard]]
auto system_error(const std::string_view &format, Ts &&...ts) -> Status
{
    return Status::system_error(fmt::format(format, std::forward<Ts>(ts)...));
}

template<class ...Ts>
[[nodiscard]]
auto logic_error(const std::string_view &format, Ts &&...ts) -> Status
{
    return Status::logic_error(fmt::format(format, std::forward<Ts>(ts)...));
}

template<class ...Ts>
[[nodiscard]]
auto corruption(const std::string_view &format, Ts &&...ts) -> Status
{
    return Status::corruption(fmt::format(format, std::forward<Ts>(ts)...));
}

template<class ...Ts>
[[nodiscard]]
auto not_found(const std::string_view &format, Ts &&...ts) -> Status
{
    return Status::not_found(fmt::format(format, std::forward<Ts>(ts)...));
}

} // namespace Calico

#endif // CALICO_UTILS_H
