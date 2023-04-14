// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_SLICE_H
#define CALICODB_SLICE_H

#include <cassert>
#include <cstring>
#include <string>

namespace calicodb
{

class Slice
{
public:
    constexpr Slice() noexcept = default;

    constexpr Slice(const char *data, std::size_t size) noexcept
        : m_data(data),
          m_size(size)
    {
        assert(m_data != nullptr);
    }

    constexpr Slice(const char *data) noexcept
        : m_data(data)
    {
        assert(m_data != nullptr);
        m_size = std::char_traits<char>::length(m_data);
    }

    constexpr Slice(const std::string_view &rhs) noexcept
        : Slice(rhs.data(), rhs.size())
    {
    }

    Slice(const std::string &rhs) noexcept
        : Slice(rhs.data(), rhs.size())
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

    [[nodiscard]] constexpr auto starts_with(const Slice &rhs) const noexcept -> bool
    {
        if (rhs.size() > m_size) {
            return false;
        }
        return std::memcmp(m_data, rhs.data(), rhs.size()) == 0;
    }

    [[nodiscard]] auto compare(const Slice &rhs) const noexcept -> int
    {
        const auto min_length = m_size < rhs.size() ? m_size : rhs.size();
        const auto r = std::memcmp(m_data, rhs.data(), min_length);
        if (r == 0) {
            if (m_size < rhs.size()) {
                return -1;
            } else if (m_size > rhs.size()) {
                return 1;
            }
        }
        return r;
    }

    [[nodiscard]] auto to_string() const noexcept -> std::string
    {
        return {m_data, m_size};
    }

private:
    const char *m_data = "";
    std::size_t m_size = 0;
};

inline auto operator<(const Slice &lhs, const Slice &rhs) noexcept -> bool
{
    return lhs.compare(rhs) < 0;
}

inline auto operator<=(const Slice &lhs, const Slice &rhs) noexcept -> bool
{
    return lhs.compare(rhs) <= 0;
}

inline auto operator>(const Slice &lhs, const Slice &rhs) noexcept -> bool
{
    return lhs.compare(rhs) > 0;
}

inline auto operator>=(const Slice &lhs, const Slice &rhs) noexcept -> bool
{
    return lhs.compare(rhs) >= 0;
}

inline auto operator==(const Slice &lhs, const Slice &rhs) noexcept -> bool
{
    return lhs.compare(rhs) == 0;
}

inline auto operator!=(const Slice &lhs, const Slice &rhs) noexcept -> bool
{
    return lhs.compare(rhs) != 0;
}

} // namespace calicodb

#endif // CALICODB_SLICE_H
