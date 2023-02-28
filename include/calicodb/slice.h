/*
 * Slice objects based off of https://github.com/google/leveldb/blob/main/include/leveldb/slice.h.
 */

#ifndef CALICODB_SLICE_H
#define CALICODB_SLICE_H

#include <cassert>
#include <cstring>
#include <string>

namespace calicodb
{

enum class Comparison {
    Less = -1,
    Equal = 0,
    Greater = 1,
};

class Slice
{
public:
    constexpr Slice() noexcept = default;

    constexpr Slice(const char *data, std::size_t size) noexcept
        : m_data {data},
          m_size {size}
    {
        assert(m_data != nullptr);
    }

    constexpr Slice(const char *data) noexcept
        : m_data {data}
    {
        assert(m_data != nullptr);
        m_size = std::char_traits<char>::length(m_data);
    }

    constexpr Slice(const std::string_view &rhs) noexcept
        : Slice {rhs.data(), rhs.size()}
    {
    }

    Slice(const std::string &rhs) noexcept
        : Slice {rhs.data(), rhs.size()}
    {
    }

    [[nodiscard]] constexpr auto is_empty() const noexcept -> bool
    {
        return m_size == 0;
    }

    [[nodiscard]] constexpr auto data() const noexcept -> const char *
    {
        return m_data;
    }

    [[nodiscard]] constexpr auto size() const noexcept -> std::size_t
    {
        return m_size;
    }

    constexpr auto operator[](std::size_t index) const noexcept -> const char &
    {
        assert(index < m_size);
        return m_data[index];
    }

    [[nodiscard]] constexpr auto range(std::size_t offset, std::size_t size) const noexcept -> Slice
    {
        assert(size <= m_size);
        assert(offset <= m_size);
        assert(offset + size <= m_size);
        return {m_data + offset, size};
    }

    [[nodiscard]] constexpr auto range(std::size_t offset) const noexcept -> Slice
    {
        assert(offset <= m_size);
        return range(offset, m_size - offset);
    }

    constexpr auto clear() noexcept -> void
    {
        m_data = "";
        m_size = 0;
    }

    constexpr auto advance(std::size_t n = 1) noexcept -> Slice
    {
        assert(n <= m_size);
        m_data += n;
        m_size -= n;
        return *this;
    }

    constexpr auto truncate(std::size_t size) noexcept -> Slice
    {
        assert(size <= m_size);
        m_size = size;
        return *this;
    }

    [[nodiscard]] constexpr auto starts_with(Slice rhs) const noexcept -> bool
    {
        if (rhs.size() > m_size) {
            return false;
        }
        return std::memcmp(m_data, rhs.data(), rhs.size()) == 0;
    }

    [[nodiscard]] auto to_string() const noexcept -> std::string
    {
        return {m_data, m_size};
    }

private:
    const char *m_data {""};
    std::size_t m_size {};
};

/*
 * Three-way comparison based off the one in LevelDB's slice.h.
 */
inline auto compare_three_way(Slice lhs, Slice rhs) noexcept -> Comparison
{
    const auto min_length = lhs.size() < rhs.size() ? lhs.size() : rhs.size();
    auto r = std::memcmp(lhs.data(), rhs.data(), min_length);
    if (r == 0) {
        if (lhs.size() < rhs.size()) {
            r = -1;
        } else if (lhs.size() > rhs.size()) {
            r = 1;
        } else {
            return Comparison::Equal;
        }
    }
    return r < 0 ? Comparison::Less : Comparison::Greater;
}

inline auto operator<(Slice lhs, Slice rhs) noexcept -> bool
{
    return compare_three_way(lhs, rhs) == Comparison::Less;
}

inline auto operator<=(Slice lhs, Slice rhs) noexcept -> bool
{
    return compare_three_way(lhs, rhs) != Comparison::Greater;
}

inline auto operator>(Slice lhs, Slice rhs) noexcept -> bool
{
    return compare_three_way(lhs, rhs) == Comparison::Greater;
}

inline auto operator>=(Slice lhs, Slice rhs) noexcept -> bool
{
    return compare_three_way(lhs, rhs) != Comparison::Less;
}

inline auto operator==(Slice lhs, Slice rhs) noexcept -> bool
{
    return compare_three_way(lhs, rhs) == Comparison::Equal;
}

inline auto operator!=(Slice lhs, Slice rhs) noexcept -> bool
{
    return compare_three_way(lhs, rhs) != Comparison::Equal;
}

} // namespace calicodb

#endif // CALICODB_SLICE_H
