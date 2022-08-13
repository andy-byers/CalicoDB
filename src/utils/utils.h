#ifndef CALICO_UTILS_UTILS_H
#define CALICO_UTILS_UTILS_H

#include "expect.h"
#include "calico/bytes.h"
#include <filesystem>

namespace calico {

static constexpr Size PAGE_ID_SIZE {sizeof(std::uint64_t)};
static constexpr Size CELL_POINTER_SIZE {sizeof(std::uint16_t)};

static constexpr Size MIN_CELL_HEADER_SIZE = sizeof(std::uint16_t) + // Key size       (2B)
                                             sizeof(std::uint32_t);  // Value size     (4B)

static constexpr Size MAX_CELL_HEADER_SIZE = MIN_CELL_HEADER_SIZE +
                                             PAGE_ID_SIZE + // Left child ID  (4B)
                                             PAGE_ID_SIZE;  // Overflow ID    (4B)

enum class PageType : std::uint16_t {
    INTERNAL_NODE = 0x494E, // "IN"
    EXTERNAL_NODE = 0x4558, // "EX"
    OVERFLOW_LINK = 0x4F56, // "OV"
    FREELIST_LINK = 0x4652, // "FR"
};

inline auto Record::operator<(const Record &rhs) const -> bool
{
    return stob(key) < stob(rhs.key);
}

inline constexpr auto is_page_type_valid(PageType type) -> bool
{
    return type == PageType::INTERNAL_NODE ||
           type == PageType::EXTERNAL_NODE ||
           type == PageType::OVERFLOW_LINK ||
           type == PageType::FREELIST_LINK;
}

// Source: http://graphics.stanford.edu/~seander/bithacks.html#DetermineIfPowerOf2
template<class T>
constexpr auto is_power_of_two(T v) noexcept -> bool
{
    return v && !(v & (v - 1));
}

inline auto mem_copy(Bytes dst, BytesView src, size_t n) noexcept -> void *
{
    CALICO_EXPECT_LE(n, src.size());
    CALICO_EXPECT_LE(n, dst.size());
    return std::memcpy(dst.data(), src.data(), n);
}

inline auto mem_copy(Bytes dst, BytesView src) noexcept -> void *
{
    CALICO_EXPECT_LE(src.size(), dst.size());
    return mem_copy(dst, src, src.size());
}

inline auto mem_clear(Bytes mem, size_t n) noexcept -> void *
{
    CALICO_EXPECT_LE(n, mem.size());
    return std::memset(mem.data(), 0, n);
}

inline auto mem_clear(Bytes mem) noexcept -> void *
{
    return mem_clear(mem, mem.size());
}

inline auto mem_move(Bytes dst, BytesView src, Size n) noexcept -> void *
{
    CALICO_EXPECT_LE(n, src.size());
    CALICO_EXPECT_LE(n, dst.size());
    return std::memmove(dst.data(), src.data(), n);
}

inline auto mem_move(Bytes dst, BytesView src) noexcept -> void *
{
    CALICO_EXPECT_LE(src.size(), dst.size());
    return mem_move(dst, src, src.size());
}

inline auto mem_clear_safe(Bytes data, Size n) noexcept -> void *
{
    CALICO_EXPECT_LE(n, data.size());
    volatile auto *p = data.data();
    for (Size i {}; i < n; ++i)
        *p++ = 0;
    return data.data();
}

inline auto mem_clear_safe(Bytes data) noexcept -> void *
{
    return mem_clear_safe(data, data.size());
}

// Modified from an answer to https://stackoverflow.com/questions/29779825 by T.C..
// Rather than depending on an additional template parameter, we restrict usage to
// containers that expose a value_type member.
template<class Container>
using ElementOf = std::conditional_t<
    std::is_lvalue_reference<Container>::value,
    typename std::remove_reference_t<Container>::value_type&,
    typename std::remove_reference_t<Container>::value_type&&>;

template<class Container, class T>
[[nodiscard]]
constexpr auto forward_like(T &&t) -> ElementOf<Container>
{
    return std::forward<ElementOf<Container>>(std::forward<T>(t));
}

} // namespace cco

#endif // CALICO_UTILS_UTILS_H
