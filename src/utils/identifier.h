#ifndef CCO_UTILS_IDENTIFIER_H
#define CCO_UTILS_IDENTIFIER_H

#include "expect.h"
#include "utils.h"

namespace cco {

template<class T>
struct Identifier {
    struct Hash {
        auto operator()(const Identifier<T> &id) const -> std::size_t
        {
            return id.value;
        }
    };

    constexpr Identifier() noexcept = default;

    template<class Id>
    constexpr explicit Identifier(Id id) noexcept:
          value {T(id)} {}

    template<class Id>
    auto operator==(const Id &rhs) const noexcept -> bool
    {
        return value == T(rhs);
    }

    template<class Id>
    auto operator!=(const Id &rhs) const noexcept -> bool
    {
        return value != T(rhs);
    }

    template<class Id>
    auto operator<(const Id &rhs) const noexcept -> bool
    {
        return value < T(rhs);
    }

    template<class Id>
    auto operator<=(const Id &rhs) const noexcept -> bool
    {
        return value <= T(rhs);
    }

    template<class Id>
    auto operator>(const Id &rhs) const noexcept -> bool
    {
        return value > T(rhs);
    }

    template<class Id>
    auto operator>=(const Id &rhs) const noexcept -> bool
    {
        return value >= T(rhs);
    }

    template<class Id>
    auto operator+=(const Id &rhs) noexcept -> Identifier&
    {
        value += T(rhs);
        return *this;
    }

    template<class Id>
    auto operator-=(const Id &rhs) noexcept -> Identifier&
    {
        CCO_EXPECT_GE(value, T(rhs));
        value -= T(rhs);
        return *this;
    }

    auto operator++() -> Identifier &
    {
        value++;
        return *this;
    }

    auto operator--() -> Identifier &
    {
        value--;
        return *this;
    }

    auto operator++(int) -> Identifier
    {
        const auto temp = *this;
        ++*this;
        return temp;
    }

    auto operator--(int) -> Identifier
    {
        const auto temp = *this;
        --*this;
        return temp;
    }

    explicit operator T() const
    {
        return value;
    }

    [[nodiscard]]
    static auto null() noexcept -> Identifier
    {
        return Identifier {T{}};
    }

    [[nodiscard]]
    static auto base() noexcept -> Identifier
    {
        return ++null();
    }

    [[nodiscard]]
    auto is_base() const noexcept -> bool
    {
        return value == base().value;
    }

    [[nodiscard]]
    auto is_null() const noexcept -> bool
    {
        return value == null().value;
    }

    [[nodiscard]]
    auto as_index() const noexcept -> Index
    {
        CCO_EXPECT_NE(value, T {});
        return value - base().value;
    }

    [[nodiscard]]
    static auto from_index(Index index) noexcept -> Identifier
    {
        return Identifier {index + 1};
    }

    T value {};
};

using PageId = Identifier<std::uint32_t>;
using SequenceNumber = Identifier<std::uint32_t>;

} // namespace cco

#endif // CCO_UTILS_IDENTIFIER_H
