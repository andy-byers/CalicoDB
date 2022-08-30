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

    template<class T, class Value>
    class SliceTraits {
    public:
        [[nodiscard]]
        constexpr auto is_empty() const noexcept -> bool
        {
            return self().size() == 0;
        }

        constexpr auto operator[](Size index) const noexcept -> const Value&
        {
            assert(index < self().size());
            return self().data()[index];
        }

        constexpr auto operator[](Size index) noexcept -> Value&
        {
            assert(index < self().size());
            return self().data()[index];
        }

        [[nodiscard]]
        constexpr auto range(Size offset, Size size) const noexcept -> T
        {
            assert(size <= self().size());
            assert(offset <= self().size());
            assert(offset + size <= self().size());

            // TODO: Is this a valid use of const_cast()? Bytes instances must be constructed with a non-const pointer to Byte, but this method is
            //       const. In Bytes, the range we get back can be used to change the underlying data, but the act of creating the range itself does
            //       not modify anything. We're really just casting our data pointer back to its own type. We could easily give Bytes and BytesView
            //       their own copies of this method with a bit more code.
            return T {const_cast<Value*>(self().data()) + offset, size};
        }

        [[nodiscard]]
        constexpr auto range(Size offset) const noexcept -> T
        {
            assert(self().size() >= offset);
            return range(offset, self().size() - offset);
        }

        [[nodiscard]]
        constexpr auto copy() const -> T
        {
            return self();
        }

        constexpr auto clear() noexcept -> void
        {
            auto &base = self();
            base.set_data(nullptr);
            base.set_size(0);
        }

        constexpr auto advance(Size n = 1) noexcept -> T&
        {
            assert(n <= self().size());
            self().set_data(self().data() + n);
            self().set_size(self().size() - n);
            return self();
        }

        constexpr auto truncate(Size size) noexcept -> T&
        {
            assert(size <= self().size());
            self().set_size(size);
            return self();
        }

        template<class S>
        [[nodiscard]]
        constexpr auto starts_with(const S &rhs) const noexcept -> bool
        {
            if (rhs.size() > self().size())
                return false;
            return std::memcmp(self().data(), rhs.data(), rhs.size()) == 0;
        }

        [[nodiscard]]
        auto to_string() const noexcept -> std::string
        {
            return std::string(self().data(), self().size());
        }

    private:
        [[nodiscard]]
        auto self() -> T&
        {
            return static_cast<T&>(*this);
        }

        [[nodiscard]]
        auto self() const -> const T&
        {
            return static_cast<const T&>(*this);
        }
    };

} // namespace impl

class Bytes: public impl::SliceTraits<Bytes, Byte> {
public:
    constexpr Bytes() noexcept = default;

    constexpr Bytes(Byte *data) noexcept
        : Bytes {data, std::strlen(data)} {}

    constexpr Bytes(Byte *data, Size size) noexcept
        : m_data {data},
          m_size {size} {}

    template<class T>
    constexpr Bytes(T &rhs) noexcept
        : Bytes {rhs.data(), rhs.size()} {}

    [[nodiscard]]
    auto data() -> Byte*
    {
        return m_data;
    }

    [[nodiscard]]
    auto data() const -> const Byte*
    {
        return m_data;
    }

    [[nodiscard]]
    auto size() const -> Size
    {
        return m_size;
    }

private:
    friend class impl::SliceTraits<Bytes, Byte>;

    auto set_data(Byte *data) -> void
    {
        m_data = data;
    }

    auto set_size(Size size) -> void
    {
        m_size = size;
    }

    Byte *m_data {};
    Size m_size {};
};

class BytesView: public impl::SliceTraits<BytesView, const Byte> {
public:
    constexpr BytesView() noexcept = default;

    constexpr BytesView(const Byte *data) noexcept
        : BytesView {data, std::strlen(data)} {}

    constexpr BytesView(const Byte *data, Size size) noexcept
        : m_data {data},
          m_size {size} {}

    template<class T>
    constexpr BytesView(const T &rhs) noexcept
        : BytesView {rhs.data(), rhs.size()} {}

    [[nodiscard]]
    auto data() const -> const Byte*
    {
        return m_data;
    }

    [[nodiscard]]
    auto size() const -> Size
    {
        return m_size;
    }

private:
    friend class impl::SliceTraits<BytesView, const Byte>;

    auto set_data(const Byte *data) -> void
    {
        m_data = data;
    }

    auto set_size(Size size) -> void
    {
        m_size = size;
    }


    const Byte *m_data {};
    Size m_size {};
};

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
