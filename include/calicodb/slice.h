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

class Slice final
{
public:
    constexpr Slice() = default;

    constexpr Slice(const char *data, size_t size)
        : m_data(data),
          m_size(size)
    {
        assert(m_data != nullptr);
    }

    constexpr Slice(const char *data)
        : m_data(data)
    {
        assert(m_data != nullptr);
        m_size = std::char_traits<char>::length(m_data);
    }

    constexpr Slice(const std::string_view &rhs)
        : Slice(rhs.data(), rhs.size())
    {
    }

    Slice(const std::string &rhs)
        : Slice(rhs.data(), rhs.size())
    {
    }

    [[nodiscard]] constexpr auto is_empty() const -> bool
    {
        return m_size == 0;
    }

    [[nodiscard]] constexpr auto data() const -> const char *
    {
        return m_data;
    }

    [[nodiscard]] constexpr auto size() const -> size_t
    {
        return m_size;
    }

    constexpr auto operator[](size_t index) const -> const char &
    {
        assert(index < m_size);
        return m_data[index];
    }

    [[nodiscard]] constexpr auto range(size_t offset, size_t size) const -> Slice
    {
        assert(size <= m_size);
        assert(offset <= m_size);
        assert(offset + size <= m_size);
        return {m_data + offset, size};
    }

    [[nodiscard]] constexpr auto range(size_t offset) const -> Slice
    {
        assert(offset <= m_size);
        return range(offset, m_size - offset);
    }

    constexpr auto clear() -> void
    {
        m_data = "";
        m_size = 0;
    }

    constexpr auto advance(size_t n = 1) -> Slice
    {
        assert(n <= m_size);
        m_data += n;
        m_size -= n;
        return *this;
    }

    constexpr auto truncate(size_t size) -> Slice
    {
        assert(size <= m_size);
        m_size = size;
        return *this;
    }

    [[nodiscard]] auto starts_with(const Slice &rhs) const -> bool
    {
        if (rhs.size() > m_size) {
            return false;
        }
        return std::memcmp(m_data, rhs.data(), rhs.size()) == 0;
    }

    [[nodiscard]] auto compare(const Slice &rhs) const -> int
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

    // Convenience conversion to std::string
    // Memory allocated for the std::string is not tracked by the allocation subsystem. Also, the
    // constructor of std::string throws std::bad_alloc. If this happens, std::terminate() will be
    // called, since CalicoDB is compiled without exceptions.
    [[nodiscard]] auto to_string() const -> std::string
    {
        return {m_data, m_size};
    }

private:
    const char *m_data = "";
    size_t m_size = 0;
};

inline auto operator<(const Slice &lhs, const Slice &rhs) -> bool
{
    return lhs.compare(rhs) < 0;
}

inline auto operator<=(const Slice &lhs, const Slice &rhs) -> bool
{
    return lhs.compare(rhs) <= 0;
}

inline auto operator>(const Slice &lhs, const Slice &rhs) -> bool
{
    return lhs.compare(rhs) > 0;
}

inline auto operator>=(const Slice &lhs, const Slice &rhs) -> bool
{
    return lhs.compare(rhs) >= 0;
}

inline auto operator==(const Slice &lhs, const Slice &rhs) -> bool
{
    return lhs.compare(rhs) == 0;
}

inline auto operator!=(const Slice &lhs, const Slice &rhs) -> bool
{
    return lhs.compare(rhs) != 0;
}

} // namespace calicodb

#endif // CALICODB_SLICE_H
