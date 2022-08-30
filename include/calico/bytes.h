/*
 * (1) https://github.com/google/leveldb/blob/main/include/leveldb/slice.h
 */

#ifndef CALICO_BYTES_H
#define CALICO_BYTES_H

#include <cassert>
#include <cstring>
#include "options.h"

namespace calico {

enum class ThreeWayComparison {
    LT = -1,
    EQ = 0,
    GT = 1,
};

namespace impl {

    template<class Pointer>
    class Slice {
    public:
        using Value = std::remove_pointer_t<Pointer>;

        constexpr Slice() noexcept = default;

        template<class Q>
        constexpr Slice(Slice<Q> rhs) noexcept
            : Slice {rhs.data(), rhs.size()} {}

        template<class Q>
        constexpr Slice(Q data, Size size) noexcept
            : m_data {data},
              m_size {size} {}

        constexpr auto operator[](Size index) const noexcept -> const Value&
        {
            assert(index < m_size);
            return m_data[index];
        }

        constexpr auto operator[](Size index) noexcept -> Value&
        {
            assert(index < m_size);
            return m_data[index];
        }

        [[nodiscard]]
        constexpr auto is_empty() const noexcept -> bool
        {
            return m_size == 0;
        }

        [[nodiscard]]
        constexpr auto size() const noexcept -> Size
        {
            return m_size;
        }

        [[nodiscard]]
        constexpr auto copy() const -> Slice
        {
            return *this;
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
        constexpr auto range(Size offset) const noexcept -> Slice
        {
            assert(m_size >= offset);
            return range(offset, m_size - offset);
        }

        [[nodiscard]]
        constexpr auto data() const noexcept -> Pointer
        {
            return m_data;
        }

        [[nodiscard]]
        constexpr auto data() noexcept -> Pointer
        {
            return m_data;
        }

        constexpr auto clear() noexcept -> void
        {
            m_data = nullptr;
            m_size = 0;
        }

        constexpr auto advance(Size n = 1) noexcept -> Slice&
        {
            assert(n <= m_size);
            m_data += n;
            m_size -= n;
            return *this;
        }

        constexpr auto truncate(Size size) noexcept -> Slice&
        {
            assert(size <= m_size);
            m_size = size;
            return *this;
        }

        template<class T>
        [[nodiscard]]
        constexpr auto starts_with(const T &rhs) const noexcept -> bool
        {
            if (rhs.size() > m_size)
                return false;
            return std::memcmp(m_data, rhs.data(), rhs.size()) == 0;
        }

    private:
        Pointer m_data {};
        Size m_size {};
    };

} // namespace impl

using Bytes = impl::Slice<Byte*>;
using BytesView = impl::Slice<const Byte*>;

inline auto stob(const std::string &data) noexcept -> BytesView
{
   return {data.data(), data.size()};
}

inline auto stob(const std::string_view &data) noexcept -> BytesView
{
    return {data.data(), data.size()};
}

inline auto stob(std::string &data) noexcept -> Bytes
{
   return {data.data(), data.size()};
}

inline auto stob(char *data) noexcept -> Bytes
{
    return {data, std::strlen(data)};
}

inline auto stob(const char *data) noexcept -> BytesView
{
    return {data, std::strlen(data)};
}

inline auto btos(BytesView data) -> std::string_view
{
   return {data.data(), data.size()};
}

inline auto compare_three_way(BytesView lhs, BytesView rhs) noexcept -> ThreeWayComparison
{
   const auto min_length = lhs.size() < rhs.size() ? lhs.size() : rhs.size();
   auto r = std::memcmp(lhs.data(), rhs.data(), min_length);
   if (r == 0) {
       if (lhs.size() < rhs.size()) {
           r = -1;
       } else if (lhs.size() > rhs.size()) {
           r = 1;
       } else {
           return ThreeWayComparison::EQ;
       }
   }
   return r < 0 ? ThreeWayComparison::LT : ThreeWayComparison::GT;
}

} // namespace calico

inline auto operator<(calico::BytesView lhs, calico::BytesView rhs) noexcept -> bool
{
    return calico::compare_three_way(lhs, rhs) == calico::ThreeWayComparison::LT;
}

inline auto operator<=(calico::BytesView lhs, calico::BytesView rhs) noexcept -> bool
{
    return calico::compare_three_way(lhs, rhs) != calico::ThreeWayComparison::GT;
}

inline auto operator>(calico::BytesView lhs, calico::BytesView rhs) noexcept -> bool
{
    return calico::compare_three_way(lhs, rhs) == calico::ThreeWayComparison::GT;
}

inline auto operator>=(calico::BytesView lhs, calico::BytesView rhs) noexcept -> bool
{
    return calico::compare_three_way(lhs, rhs) != calico::ThreeWayComparison::LT;
}

inline auto operator==(calico::BytesView lhs, calico::BytesView rhs) noexcept -> bool
{
    return calico::compare_three_way(lhs, rhs) == calico::ThreeWayComparison::EQ;
}

inline auto operator!=(calico::BytesView lhs, calico::BytesView rhs) noexcept -> bool
{
    return !(lhs == rhs);
}

#endif // CALICO_BYTES_H
