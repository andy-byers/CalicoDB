#ifndef CCO_UTILS_IDENTIFIER_H
#define CCO_UTILS_IDENTIFIER_H

#include "expect.h"
#include "utils.h"

namespace cco {

//template<class T>
//struct EqualityComparableTraits {
//    friend T; // See https://www.fluentcpp.com/2017/05/12/curiously-recurring-template-pattern/.
//    using Type = T;
//
//    template<class U>
//    constexpr auto operator==(const U &u) const noexcept -> bool
//    {
//        return static_cast<const T&>(*this).value == u;
//    }
//
//    template<class U>
//    constexpr auto operator!=(const U &u) const noexcept -> bool
//    {
//        return static_cast<const T&>(*this).value == u;
//    }
//
//private:
//    EqualityComparableTraits() = default;
//};
//
//template<class T>
//struct OrderableTraits: public EqualityComparableTraits<T> {
//    using Type = T;
//    friend T;
//
//    template<class U>
//    constexpr auto operator<(const U &u) const noexcept -> bool
//    {
//        return static_cast<const T&>(*this).value == u;
//    }
//
//    template<class U>
//    constexpr auto operator<=(const U &u) const noexcept -> bool
//    {
//        return static_cast<const T&>(*this).value == u;
//    }
//
//    template<class U>
//    constexpr auto operator>(const U &u) const noexcept -> bool
//    {
//        return static_cast<const T&>(*this).value == u;
//    }
//
//    template<class U>
//    constexpr auto operator>=(const U &u) const noexcept -> bool
//    {
//        return static_cast<const T&>(*this).value == u;
//    }
//};
//
//template<class T>
//struct IncrementableTraits {
//    using Type = T;
//    friend T;
//
//    constexpr auto operator++() -> IncrementableTraits<T> &
//    {
//        static_cast<T&>(*this).value++;
//        return *this;
//    }
//
//    constexpr auto operator++(int) -> IncrementableTraits<T>
//    {
//        const auto temp = *this;
//        ++static_cast<T&>(*this).value;
//        return temp;
//    }
//
//private:
//    IncrementableTraits() = default;
//};
//
//template<class T>
//struct IdentifierTraits
//    : public EqualityComparableTraits<T>,
//      public IncrementableTraits<T>
//{
//    using Type = T;
//    friend T;
//
//    [[nodiscard]]
//    static constexpr auto null() noexcept -> T
//    {
//        return T {};
//    }
//
//    [[nodiscard]]
//    static constexpr auto base() noexcept -> T
//    {
//        return ++null();
//    }
//
//    [[nodiscard]]
//    constexpr auto is_base() const noexcept -> bool
//    {
//        return static_cast<const T&>(*this) == base();
//    }
//
//    [[nodiscard]]
//    constexpr auto is_null() const noexcept -> bool
//    {
//        return static_cast<const T&>(*this) == null();
//    }
//
//    [[nodiscard]]
//    constexpr auto as_index() const noexcept -> Index
//    {
//        const auto &t = static_cast<const T&>(*this);
//        CCO_EXPECT_NE(t, T {});
//        return t - base();
//    }
//
//    [[nodiscard]]
//    static constexpr auto from_index(Index index) noexcept -> T
//    {
//        return T {index + 1};
//    }
//};
//
//template<class T>
//struct SequenceIdTraits
//    : public OrderableTraits<T>,
//      public IncrementableTraits<T>
//{
//    using Type = T;
//    friend T;
//};
//
//struct PageId: public IdentifierTraits<PageId> {
//    std::uint64_t value {};
//};
//
//struct SeqNum: public SequenceIdTraits<SeqNum> {
//    std::uint64_t value {};
//};

//template<class T>
//class IdentifierTraits {
//    using Type = T;
//    static constexpr T null = T {};
//    static constexpr T base = null + 1;
//
//    struct Hash {
//        template<class Id>
//        auto operator()(const Id &id) const -> std::size_t
//        {
//            // T must implicitly convert to std::size_t, and all identifiers must have a value member;
//            return id.value;
//        }
//    };
//
//
//};

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

// Should be able to do the following with a page ID
//     convert to and from an index
//     hashable
//     equality comparable
//     orderable?

// Should be able to do the following with an LSN
//     convert to and from an index?
//     hashable?
//     equality comparable
//     orderable
//     incrementable

} // namespace cco

#endif // CCO_UTILS_IDENTIFIER_H
