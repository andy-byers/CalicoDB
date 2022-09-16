/*
 * Slice objects based off of https://github.com/google/leveldb/blob/main/include/leveldb/slice.h.
 */

#ifndef CALICO_BYTES_H
#define CALICO_BYTES_H

#include <cassert>
#include <cstring>
#include <string>
#include "common.h"

namespace calico {

enum class ThreeWayComparison {
    LT = -1,
    EQ = 0,
    GT = 1,
};

namespace impl {

    template<
        class SelfType,
        class ConstType,
        class ValueType
    >
    class SliceTraits {
    public:
        [[nodiscard]]
        constexpr auto is_empty() const noexcept -> bool
        {
            return self().size() == 0;
        }

        constexpr auto operator[](Size index) const noexcept -> const ValueType&
        {
            assert(index < self().size());
            return self().data()[index];
        }

        constexpr auto operator[](Size index) noexcept -> ValueType&
        {
            assert(index < self().size());
            return self().data()[index];
        }

        [[nodiscard]]
        constexpr auto range(Size offset, Size size) const noexcept -> SelfType
        {
            assert(size <= self().size());
            assert(offset <= self().size());
            assert(offset + size <= self().size());

            // TODO: Is this a valid use of const_cast()? Bytes instances must be constructed with a non-const pointer to Byte, but this method is
            //       const, causing us to hit the const overload of data(). In Bytes, the range we get back can be used to change the underlying data,
            //       but the act of creating the range itself does not modify anything. We're really just casting our data pointer back to its own
            //       type. We could easily give Bytes and BytesView their own copies of this method with a bit more code.
            return SelfType {const_cast<ValueType*>(self().data()) + offset, size};
        }

        [[nodiscard]]
        constexpr auto range(Size offset) const noexcept -> SelfType
        {
            assert(offset <= self().size());
            return range(offset, self().size() - offset);
        }

        [[nodiscard]]
        constexpr auto copy() const noexcept -> SelfType
        {
            return self();
        }

        constexpr auto clear() noexcept -> void
        {
            auto &base = self();
            base.set_data(nullptr);
            base.set_size(0);
        }

        constexpr auto advance(Size n = 1) noexcept -> SelfType&
        {
            assert(n <= self().size());
            self().set_data(self().data() + n);
            self().set_size(self().size() - n);
            return self();
        }

        constexpr auto truncate(Size size) noexcept -> SelfType&
        {
            assert(size <= self().size());
            self().set_size(size);
            return self();
        }

        [[nodiscard]]
        constexpr auto starts_with(const ValueType *rhs) const noexcept -> bool
        {
            // NOTE: rhs must be null-terminated.
            const auto size = std::char_traits<ValueType>::length(rhs);
            if (size > self().size())
                return false;
            return std::memcmp(self().data(), rhs, size) == 0;
        }

        [[nodiscard]]
        constexpr auto starts_with(ConstType rhs) const noexcept -> bool
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
        constexpr auto self() -> SelfType&
        {
            return static_cast<SelfType&>(*this);
        }

        [[nodiscard]]
        constexpr auto self() const -> const SelfType&
        {
            return static_cast<const SelfType&>(*this);
        }

        friend SelfType;
        SliceTraits() = default;
    };

} // namespace impl

class Bytes;

class BytesView final: public impl::SliceTraits<BytesView, BytesView, const Byte> {
public:
    // Implicit conversion from Bytes to BytesView. Not allowed the other way.
    constexpr BytesView(Bytes data) noexcept;

    constexpr BytesView() noexcept = default;

    // WARNING: The data passed in here must be null-terminated.
    constexpr BytesView(const Byte *data) noexcept
        : BytesView {data, std::char_traits<Byte>::length(data)}
    {
        assert(data != nullptr);
    }

    constexpr BytesView(const Byte *data, Size size) noexcept
        : m_data {data},
          m_size {size}
    {
        assert(data != nullptr);
    }

    constexpr BytesView(std::string_view rhs) noexcept
        : BytesView {rhs.data(), rhs.size()} {}


    BytesView(const std::string &rhs) noexcept
        : BytesView {rhs.data(), rhs.size()} {}

    [[nodiscard]]
    constexpr auto data() const noexcept -> const Byte*
    {
        return m_data;
    }

    [[nodiscard]]
    constexpr auto size() const noexcept -> Size
    {
        return m_size;
    }

private:
    friend class impl::SliceTraits<BytesView, BytesView, const Byte>;

    constexpr auto set_data(const Byte *data) noexcept -> void
    {
        m_data = data;
    }

    constexpr auto set_size(Size size) noexcept -> void
    {
        m_size = size;
    }

    const Byte *m_data {};
    Size m_size {};
};

class Bytes final: public impl::SliceTraits<Bytes, BytesView, Byte> {
public:
    constexpr Bytes() noexcept = default;

    // WARNING: The data passed in here must be null-terminated.
    constexpr Bytes(Byte *data) noexcept
        : Bytes {data, std::char_traits<Byte>::length(data)}
    {
        assert(data != nullptr);
    }

    constexpr Bytes(Byte *data, Size size) noexcept
        : m_data {data},
          m_size {size}
    {
        assert(data != nullptr);
    }

    Bytes(std::string &rhs) noexcept
        : Bytes {rhs.data(), rhs.size()} {}

    [[nodiscard]]
    constexpr auto data() noexcept -> Byte*
    {
        return m_data;
    }

    [[nodiscard]]
    constexpr auto data() const noexcept -> const Byte*
    {
        return m_data;
    }

    [[nodiscard]]
    constexpr auto size() const noexcept -> Size
    {
        return m_size;
    }

private:
    friend class impl::SliceTraits<Bytes, BytesView, Byte>;

    constexpr auto set_data(Byte *data) noexcept -> void
    {
        m_data = data;
    }

    constexpr auto set_size(Size size) noexcept -> void
    {
        m_size = size;
    }

    Byte *m_data {};
    Size m_size {};
};

constexpr BytesView::BytesView(Bytes data) noexcept
    : BytesView {data.data(), data.size()} {}

inline auto stob(std::string &data) noexcept -> Bytes
{
    return {data.data(), data.size()};
}

inline constexpr auto stob(char *data) noexcept -> Bytes
{
    return {data};
}

inline constexpr auto stob(std::string_view data) noexcept -> BytesView
{
    return {data};
}

inline constexpr auto stob(const char *data) noexcept -> BytesView
{
    return {data};
}

/*
 * Three-way comparison based off the one in LevelDB's slice.h.
 */
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
