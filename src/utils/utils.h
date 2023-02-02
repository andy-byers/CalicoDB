#ifndef CALICO_UTILS_H
#define CALICO_UTILS_H

#include "calico/slice.h"
#include "expect.h"
#include <filesystem>
#include <spdlog/fmt/fmt.h>

namespace Calico {

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

// Source: http://graphics.stanford.edu/~seander/bithacks.html#DetermineIfPowerOf2
template<class T>
constexpr auto is_power_of_two(T v) noexcept -> bool
{
    return v && !(v & (v - 1));
}

class Span {
public:
    constexpr Span() noexcept = default;

    constexpr Span(Byte *data, Size size) noexcept
        : m_data {data},
          m_size {size}
    {
        CALICO_EXPECT_NE(m_data, nullptr);
    }

    constexpr Span(Byte *data) noexcept
        : m_data {data}
    {
        CALICO_EXPECT_NE(m_data, nullptr);
        m_size = std::char_traits<Byte>::length(m_data);
    }

    Span(std::string &rhs) noexcept
        : Span {rhs.data(), rhs.size()} {}

    [[nodiscard]]
    constexpr auto is_empty() const noexcept -> bool
    {
        return m_size == 0;
    }

    [[nodiscard]]
    constexpr auto data() noexcept -> Byte *
    {
        return m_data;
    }

    [[nodiscard]]
    constexpr auto data() const noexcept -> const Byte *
    {
        return m_data;
    }

    [[nodiscard]]
    constexpr auto size() const noexcept -> Size
    {
        return m_size;
    }

    constexpr operator Slice() const
    {
        return {m_data, m_size};
    }

    constexpr auto operator[](Size index) const noexcept -> const Byte &
    {
        assert(index < m_size);
        return m_data[index];
    }

    constexpr auto operator[](Size index) noexcept -> Byte &
    {
        assert(index < m_size);
        return m_data[index];
    }

    [[nodiscard]]
    constexpr auto range(Size offset, Size size) const noexcept -> Slice
    {
        assert(size <= m_size);
        assert(offset <= m_size);
        assert(offset + size <= m_size);

        return Slice {m_data + offset, size};
    }

    [[nodiscard]]
    constexpr auto range(Size offset, Size size) noexcept -> Span
    {
        assert(size <= m_size);
        assert(offset <= m_size);
        assert(offset + size <= m_size);

        return Span {m_data + offset, size};
    }

    [[nodiscard]]
    constexpr auto range(Size offset) const noexcept -> Slice
    {
        assert(offset <= m_size);
        return range(offset, m_size - offset);
    }

    [[nodiscard]]
    constexpr auto range(Size offset) noexcept -> Span
    {
        assert(offset <= m_size);
        return range(offset, m_size - offset);
    }

    [[nodiscard]]
    constexpr auto copy() const noexcept -> Span
    {
        return *this;
    }

    constexpr auto clear() noexcept -> void
    {
        m_data = nullptr;
        m_size = 0;
    }

    constexpr auto advance(Size n = 1) noexcept -> Span
    {
        assert(n <= m_size);
        m_data += n;
        m_size -= n;
        return *this;
    }

    constexpr auto truncate(Size size) noexcept -> Span
    {
        assert(size <= m_size);
        m_size = size;
        return *this;
    }

    [[nodiscard]]
    constexpr auto starts_with(const Byte *rhs) const noexcept -> bool
    {
        // NOTE: rhs must be null-terminated.
        const auto size = std::char_traits<Byte>::length(rhs);
        if (size > m_size)
            return false;
        return std::memcmp(m_data, rhs, size) == 0;
    }

    [[nodiscard]]
    constexpr auto starts_with(const Slice &rhs) const noexcept -> bool
    {
        if (rhs.size() > m_size)
            return false;
        return std::memcmp(m_data, rhs.data(), rhs.size()) == 0;
    }

    [[nodiscard]]
    auto to_string() const noexcept -> std::string
    {
        return {m_data, m_size};
    }

private:
    Byte *m_data {};
    Size m_size {};
};

inline auto mem_copy(Span dst, const Slice &src, size_t n) noexcept -> void *
{
    CALICO_EXPECT_LE(n, src.size());
    CALICO_EXPECT_LE(n, dst.size());
    return std::memcpy(dst.data(), src.data(), n);
}

inline auto mem_copy(Span dst, const Slice &src) noexcept -> void *
{
    CALICO_EXPECT_LE(src.size(), dst.size());
    return mem_copy(dst, src, src.size());
}

inline auto mem_clear(Span mem, size_t n) noexcept -> void *
{
    CALICO_EXPECT_LE(n, mem.size());
    return std::memset(mem.data(), 0, n);
}

inline auto mem_clear(Span mem) noexcept -> void *
{
    return mem_clear(mem, mem.size());
}

inline auto mem_move(Span dst, const Slice &src, Size n) noexcept -> void *
{
    CALICO_EXPECT_LE(n, src.size());
    CALICO_EXPECT_LE(n, dst.size());
    return std::memmove(dst.data(), src.data(), n);
}

inline auto mem_move(Span dst, const Slice &src) noexcept -> void *
{
    CALICO_EXPECT_LE(src.size(), dst.size());
    return mem_move(dst, src, src.size());
}

inline auto mem_clear_safe(Span data, Size n) noexcept -> void *
{
    CALICO_EXPECT_LE(n, data.size());
    volatile auto *p = data.data();
    for (Size i {}; i < n; ++i)
        *p++ = 0;
    return data.data();
}

inline auto mem_clear_safe(Span data) noexcept -> void *
{
    return mem_clear_safe(data, data.size());
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
