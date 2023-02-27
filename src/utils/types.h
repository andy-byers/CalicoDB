#ifndef CALICO_UTILS_TYPES_H
#define CALICO_UTILS_TYPES_H

#include "calico/slice.h"
#include "utils.h"
#include <utility>

namespace Calico {

struct Id {
    static constexpr Size null_value {0};
    static constexpr Size root_value {1};

    struct Hash {
        auto operator()(const Id &id) const -> Size
        {
            return id.value;
        }
    };

    [[nodiscard]] static constexpr auto from_index(Size index) noexcept -> Id
    {
        return {index + 1};
    }

    [[nodiscard]] static constexpr auto null() noexcept -> Id
    {
        return {null_value};
    }

    [[nodiscard]] static constexpr auto root() noexcept -> Id
    {
        return {root_value};
    }

    [[nodiscard]] constexpr auto is_null() const noexcept -> bool
    {
        return value == null_value;
    }

    [[nodiscard]] constexpr auto is_root() const noexcept -> bool
    {
        return value == root_value;
    }

    [[nodiscard]] constexpr auto as_index() const noexcept -> Size
    {
        CALICO_EXPECT_NE(value, null().value);
        return value - 1;
    }

    Size value {};
};

inline auto operator<(Id lhs, Id rhs) -> bool
{
    return lhs.value < rhs.value;
}

inline auto operator>(Id lhs, Id rhs) -> bool
{
    return lhs.value > rhs.value;
}

inline auto operator<=(Id lhs, Id rhs) -> bool
{
    return lhs.value <= rhs.value;
}

inline auto operator>=(Id lhs, Id rhs) -> bool
{
    return lhs.value >= rhs.value;
}

inline auto operator==(Id lhs, Id rhs) -> bool
{
    return lhs.value == rhs.value;
}

inline auto operator!=(Id lhs, Id rhs) -> bool
{
    return lhs.value != rhs.value;
}

using Lsn = Id;

class AlignedBuffer {
public:
    AlignedBuffer(Size size, Size alignment)
        : m_data {
              new (std::align_val_t {alignment}, std::nothrow) Byte[size](),
              Deleter {std::align_val_t {alignment}},
          }
    {
        CALICO_EXPECT_TRUE(is_power_of_two(alignment));
        CALICO_EXPECT_EQ(size % alignment, 0);
    }

    [[nodiscard]] auto get() -> Byte *
    {
        return m_data.get();
    }

    [[nodiscard]] auto get() const -> const Byte *
    {
        return m_data.get();
    }

private:
    struct Deleter {
        auto operator()(Byte *ptr) const -> void
        {
            operator delete[](ptr, alignment);
        }

        std::align_val_t alignment;
    };

    std::unique_ptr<Byte[], Deleter> m_data;
};

template<class T>
class UniqueNullable final {
public:
    using Type = T;

    ~UniqueNullable() = default;
    UniqueNullable() = delete;
    UniqueNullable(const UniqueNullable &) = delete;
    auto operator=(const UniqueNullable &) -> UniqueNullable & = delete;

    template<class Resource>
    explicit UniqueNullable(Resource resource)
        : m_resource {resource}
    {}

    UniqueNullable(UniqueNullable &&rhs) noexcept
    {
        m_resource = std::exchange(rhs.m_resource, T {});
    }

    auto operator=(UniqueNullable &&rhs) noexcept -> UniqueNullable &
    {
        m_resource = rhs.reset();
        return *this;
    }

    [[nodiscard]] auto is_valid() const -> bool
    {
        return m_resource;
    }

    auto reset() -> T
    {
        return std::exchange(m_resource, T {});
    }

    auto operator->() noexcept -> T &
    {
        return m_resource;
    }

    auto operator->() const noexcept -> const T &
    {
        return m_resource;
    }

    auto operator*() noexcept -> T &
    {
        return m_resource;
    }

    auto operator*() const noexcept -> const T &
    {
        return m_resource;
    }

private:
    T m_resource {};
};

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
        : Span {rhs.data(), rhs.size()}
    {}

    [[nodiscard]] constexpr auto is_empty() const noexcept -> bool
    {
        return m_size == 0;
    }

    [[nodiscard]] constexpr auto data() noexcept -> Byte *
    {
        return m_data;
    }

    [[nodiscard]] constexpr auto data() const noexcept -> const Byte *
    {
        return m_data;
    }

    [[nodiscard]] constexpr auto size() const noexcept -> Size
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

    [[nodiscard]] constexpr auto range(Size offset, Size size) const noexcept -> Slice
    {
        assert(size <= m_size);
        assert(offset <= m_size);
        assert(offset + size <= m_size);

        return Slice {m_data + offset, size};
    }

    [[nodiscard]] constexpr auto range(Size offset, Size size) noexcept -> Span
    {
        assert(size <= m_size);
        assert(offset <= m_size);
        assert(offset + size <= m_size);

        return Span {m_data + offset, size};
    }

    [[nodiscard]] constexpr auto range(Size offset) const noexcept -> Slice
    {
        assert(offset <= m_size);
        return range(offset, m_size - offset);
    }

    [[nodiscard]] constexpr auto range(Size offset) noexcept -> Span
    {
        assert(offset <= m_size);
        return range(offset, m_size - offset);
    }

    [[nodiscard]] constexpr auto copy() const noexcept -> Span
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

    [[nodiscard]] constexpr auto starts_with(const Byte *rhs) const noexcept -> bool
    {
        // NOTE: rhs must be null-terminated.
        const auto size = std::char_traits<Byte>::length(rhs);
        if (size > m_size)
            return false;
        return std::memcmp(m_data, rhs, size) == 0;
    }

    [[nodiscard]] constexpr auto starts_with(const Slice &rhs) const noexcept -> bool
    {
        if (rhs.size() > m_size)
            return false;
        return std::memcmp(m_data, rhs.data(), rhs.size()) == 0;
    }

    [[nodiscard]] auto to_string() const noexcept -> std::string
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

} // namespace Calico

#endif // CALICO_UTILS_TYPES_H
