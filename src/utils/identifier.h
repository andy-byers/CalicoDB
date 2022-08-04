#ifndef CCO_UTILS_IDENTIFIER_H
#define CCO_UTILS_IDENTIFIER_H

#include "expect.h"
#include "utils.h"

namespace cco {

template<class T>
struct Identifier {
    using Type = T;

    struct Hash {
        auto operator()(const Identifier<T> &id) const -> std::size_t
        {
            return id.value;
        }
    };

    constexpr Identifier() noexcept = default;
    constexpr auto operator=(const Identifier &rhs) -> Identifier&
    {
        value = rhs.value;
        return *this;
    }
    constexpr Identifier(const Identifier &rhs)
        : value {rhs.value}
    {}

    ~Identifier() = default;

    template<class Id>
    constexpr explicit Identifier(Id &&id) noexcept
        : value {std::forward<Id>(id)} {}

    template<class Id>
    constexpr auto operator==(const Id &rhs) const noexcept -> bool
    {
        return value == T(rhs);
    }

    template<class Id>
    constexpr auto operator!=(const Id &rhs) const noexcept -> bool
    {
        return value != T(rhs);
    }

    template<class Id>
    constexpr auto operator<(const Id &rhs) const noexcept -> bool
    {
        return value < T(rhs);
    }

    template<class Id>
    constexpr auto operator<=(const Id &rhs) const noexcept -> bool
    {
        return value <= T(rhs);
    }

    template<class Id>
    constexpr auto operator>(const Id &rhs) const noexcept -> bool
    {
        return value > T(rhs);
    }

    template<class Id>
    constexpr auto operator>=(const Id &rhs) const noexcept -> bool
    {
        return value >= T(rhs);
    }

    template<class Id>
    constexpr auto operator+=(const Id &rhs) noexcept -> Identifier&
    {
        value += T(rhs);
        return *this;
    }

    template<class Id>
    constexpr auto operator-=(const Id &rhs) noexcept -> Identifier&
    {
        CCO_EXPECT_GE(value, T(rhs));
        value -= T(rhs);
        return *this;
    }

    constexpr auto operator++() -> Identifier &
    {
        value++;
        return *this;
    }

    constexpr auto operator--() -> Identifier &
    {
        value--;
        return *this;
    }

    constexpr auto operator++(int) -> Identifier
    {
        const auto temp = *this;
        ++*this;
        return temp;
    }

    constexpr auto operator--(int) -> Identifier
    {
        const auto temp = *this;
        --*this;
        return temp;
    }

    explicit constexpr operator T() const
    {
        return value;
    }

    [[nodiscard]]
    static constexpr auto null() noexcept -> Identifier
    {
        return Identifier {T{}};
    }

    [[nodiscard]]
    static constexpr auto base() noexcept -> Identifier
    {
        return ++null();
    }

    [[nodiscard]]
    constexpr auto is_base() const noexcept -> bool
    {
        return value == base().value;
    }

    [[nodiscard]]
    constexpr auto is_null() const noexcept -> bool
    {
        return value == null().value;
    }

    [[nodiscard]]
    constexpr auto as_index() const noexcept -> Index
    {
        CCO_EXPECT_NE(value, T {});
        return value - base().value;
    }

    [[nodiscard]]
    static constexpr auto from_index(Index index) noexcept -> Identifier
    {
        return Identifier {index + 1};
    }

    T value {};
};

using PageId = Identifier<std::uint64_t>;
using SequenceNumber = Identifier<std::uint64_t>;

} // namespace cco

#endif // CCO_UTILS_IDENTIFIER_H
