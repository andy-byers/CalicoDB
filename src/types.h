#ifndef CALICODB_TYPES_H
#define CALICODB_TYPES_H

#include "calicodb/slice.h"
#include "utils.h"
#include <utility>

namespace calicodb
{

struct Id {
    static constexpr std::size_t null_value {0};
    static constexpr std::size_t root_value {1};

    struct Hash {
        auto operator()(const Id &id) const -> std::size_t
        {
            return id.value;
        }
    };

    [[nodiscard]] static constexpr auto from_index(std::size_t index) noexcept -> Id
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

    [[nodiscard]] constexpr auto as_index() const noexcept -> std::size_t
    {
        CDB_EXPECT_NE(value, null().value);
        return value - 1;
    }

    std::size_t value {};
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

class AlignedBuffer
{
public:
    AlignedBuffer(std::size_t size, std::size_t alignment)
        : m_data {
              new (std::align_val_t {alignment}, std::nothrow) char[size](),
              Deleter {std::align_val_t {alignment}},
          }
    {
        CDB_EXPECT_TRUE(is_power_of_two(alignment));
        CDB_EXPECT_EQ(size % alignment, 0);
    }

    [[nodiscard]] auto get() -> char *
    {
        return m_data.get();
    }

    [[nodiscard]] auto get() const -> const char *
    {
        return m_data.get();
    }

private:
    struct Deleter {
        auto operator()(char *ptr) const -> void
        {
            operator delete[](ptr, alignment);
        }

        std::align_val_t alignment;
    };

    std::unique_ptr<char[], Deleter> m_data;
};

template <class T>
class UniqueNullable final
{
public:
    using Type = T;

    ~UniqueNullable() = default;
    UniqueNullable() = delete;
    UniqueNullable(const UniqueNullable &) = delete;
    auto operator=(const UniqueNullable &) -> UniqueNullable & = delete;

    template <class Resource>
    explicit UniqueNullable(Resource resource)
        : m_resource {resource}
    {
    }

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

class Span
{
public:
    constexpr Span() noexcept = default;

    constexpr Span(char *data, std::size_t size) noexcept
        : m_data {data},
          m_size {size}
    {
        CDB_EXPECT_NE(m_data, nullptr);
    }

    constexpr Span(char *data) noexcept
        : m_data {data}
    {
        CDB_EXPECT_NE(m_data, nullptr);
        m_size = std::char_traits<char>::length(m_data);
    }

    Span(std::string &rhs) noexcept
        : Span {rhs.data(), rhs.size()}
    {
    }

    [[nodiscard]] constexpr auto is_empty() const noexcept -> bool
    {
        return m_size == 0;
    }

    [[nodiscard]] constexpr auto data() noexcept -> char *
    {
        return m_data;
    }

    [[nodiscard]] constexpr auto data() const noexcept -> const char *
    {
        return m_data;
    }

    [[nodiscard]] constexpr auto size() const noexcept -> std::size_t
    {
        return m_size;
    }

    constexpr operator Slice() const
    {
        return {m_data, m_size};
    }

    constexpr auto operator[](std::size_t index) const noexcept -> const char &
    {
        assert(index < m_size);
        return m_data[index];
    }

    constexpr auto operator[](std::size_t index) noexcept -> char &
    {
        assert(index < m_size);
        return m_data[index];
    }

    [[nodiscard]] constexpr auto range(std::size_t offset, std::size_t size) const noexcept -> Slice
    {
        assert(size <= m_size);
        assert(offset <= m_size);
        assert(offset + size <= m_size);

        return Slice {m_data + offset, size};
    }

    [[nodiscard]] constexpr auto range(std::size_t offset, std::size_t size) noexcept -> Span
    {
        assert(size <= m_size);
        assert(offset <= m_size);
        assert(offset + size <= m_size);

        return Span {m_data + offset, size};
    }

    [[nodiscard]] constexpr auto range(std::size_t offset) const noexcept -> Slice
    {
        assert(offset <= m_size);
        return range(offset, m_size - offset);
    }

    [[nodiscard]] constexpr auto range(std::size_t offset) noexcept -> Span
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

    constexpr auto advance(std::size_t n = 1) noexcept -> Span
    {
        assert(n <= m_size);
        m_data += n;
        m_size -= n;
        return *this;
    }

    constexpr auto truncate(std::size_t size) noexcept -> Span
    {
        assert(size <= m_size);
        m_size = size;
        return *this;
    }

    [[nodiscard]] constexpr auto starts_with(const char *rhs) const noexcept -> bool
    {
        // NOTE: rhs must be null-terminated.
        const auto size = std::char_traits<char>::length(rhs);
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
    char *m_data {};
    std::size_t m_size {};
};

inline auto mem_copy(Span dst, const Slice &src, size_t n) noexcept -> void *
{
    CDB_EXPECT_LE(n, src.size());
    CDB_EXPECT_LE(n, dst.size());
    return std::memcpy(dst.data(), src.data(), n);
}

inline auto mem_copy(Span dst, const Slice &src) noexcept -> void *
{
    CDB_EXPECT_LE(src.size(), dst.size());
    return mem_copy(dst, src, src.size());
}

inline auto mem_clear(Span mem, size_t n) noexcept -> void *
{
    CDB_EXPECT_LE(n, mem.size());
    return std::memset(mem.data(), 0, n);
}

inline auto mem_clear(Span mem) noexcept -> void *
{
    return mem_clear(mem, mem.size());
}

inline auto mem_move(Span dst, const Slice &src, std::size_t n) noexcept -> void *
{
    CDB_EXPECT_LE(n, src.size());
    CDB_EXPECT_LE(n, dst.size());
    return std::memmove(dst.data(), src.data(), n);
}

inline auto mem_move(Span dst, const Slice &src) noexcept -> void *
{
    CDB_EXPECT_LE(src.size(), dst.size());
    return mem_move(dst, src, src.size());
}

} // namespace calicodb

#endif // CALICODB_TYPES_H
