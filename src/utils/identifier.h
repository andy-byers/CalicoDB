#ifndef CCO_UTILS_IDENTIFIER_H
#define CCO_UTILS_IDENTIFIER_H

#include "expect.h"
#include "utils.h"

namespace cco {

struct IdentifierHash;

struct Identifier {
    using Hasher = IdentifierHash;

    [[nodiscard]] static auto min() noexcept -> Identifier
    {
        return Identifier {std::numeric_limits<decltype(value)>::min()};
    }

    [[nodiscard]] static auto max() noexcept -> Identifier
    {
        return Identifier {std::numeric_limits<decltype(value)>::max()};
    }

    [[nodiscard]] static auto from_index(Index index) noexcept -> Identifier
    {
        return Identifier {index + ROOT_ID_VALUE};
    }

    Identifier() noexcept = default;

    template<class Id> explicit Identifier(Id id) noexcept
        : value {static_cast<uint32_t>(id)}
    {
        CCO_EXPECT_BOUNDED_BY(uint32_t, static_cast<std::make_unsigned_t<Id>>(id));
    }

    auto operator==(const Identifier &rhs) const noexcept -> bool
    {
        return value == rhs.value;
    }

    auto operator!=(const Identifier &rhs) const noexcept -> bool
    {
        return value != rhs.value;
    }

    auto operator<(const Identifier &rhs) const noexcept -> bool
    {
        return value < rhs.value;
    }

    auto operator<=(const Identifier &rhs) const noexcept -> bool
    {
        return value <= rhs.value;
    }

    auto operator>(const Identifier &rhs) const noexcept -> bool
    {
        return value > rhs.value;
    }

    auto operator>=(const Identifier &rhs) const noexcept -> bool
    {
        return value >= rhs.value;
    }

    [[nodiscard]] auto is_null() const noexcept -> bool
    {
        return value == 0;
    }

    [[nodiscard]] auto as_index() const noexcept -> Index
    {
        CCO_EXPECT_GE(value, ROOT_ID_VALUE);
        return value - ROOT_ID_VALUE;
    }

    uint32_t value{};
};

inline auto operator+(const Identifier &lhs, const Identifier &rhs) noexcept -> Identifier
{
    Identifier res {lhs};
    res.value += rhs.value;
    return res;
}

struct IdentifierHash {
    auto operator()(const Identifier &id) const -> size_t
    {
        return id.value;
    }
};

struct PID final: public Identifier {
    PID() noexcept = default;

    PID(Identifier id) noexcept
        : Identifier {id.value} {}

    template<class T> explicit PID(T id) noexcept
        : Identifier {id} {}

    [[nodiscard]] auto is_root() const noexcept -> bool
    {
        return value == ROOT_ID_VALUE;
    }

    static auto null() noexcept -> PID
    {
        return PID {NULL_ID_VALUE};
    }

    static auto root() noexcept -> PID
    {
        return PID {ROOT_ID_VALUE};
    }
};

struct LSN final: public Identifier {
    LSN() noexcept = default;

    LSN(Identifier id) noexcept
        : Identifier {id.value} {}

    template<class T> explicit LSN(T id) noexcept
        : Identifier {id} {}

    [[nodiscard]] auto is_base() const noexcept -> bool
    {
        return value == ROOT_ID_VALUE;
    }

    static auto null() noexcept -> LSN
    {
        return LSN {NULL_ID_VALUE};
    }

    static auto base() noexcept -> LSN
    {
        return LSN {ROOT_ID_VALUE};
    }

    auto operator++() -> LSN&
    {
        value++;
        return *this;
    }

    auto operator++(int) -> LSN
    {
        const auto temp = *this;
        ++*this;
        return temp;
    }
};

} // calico

#endif // CCO_UTILS_IDENTIFIER_H
