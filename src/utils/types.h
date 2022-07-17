/**
 * References
 * (1) https://en.cppreference.com/w/cpp/utility/exchange
 */

#ifndef CCO_UTILS_TYPES_H
#define CCO_UTILS_TYPES_H

#include "calico/options.h"

namespace cco::utils {

template<class Value> struct Unique {

    template<class V> explicit Unique(V v)
        : value{std::move(v)} {}

    Unique(const Unique &) = delete;
    auto operator=(const Unique &) -> Unique & = delete;

    Unique(Unique &&rhs) noexcept
    {
        *this = std::move(rhs);
    }

    auto operator=(Unique &&rhs) noexcept -> Unique &
    {
        // TODO: std::exchange() is not noexcept until C++23, but (1) doesn't specify
        //       any exceptions it could throw. Depends on `Value`?
        value = std::exchange(rhs.value, {});
        return *this;
    }

    auto operator->() noexcept -> Value&
    {
        return value;
    }

    auto operator->() const noexcept -> const Value&
    {
        return value;
    }

    Value value;
};

template<class T>
class UniqueNullable {
public:
    UniqueNullable() = delete;
    UniqueNullable(const UniqueNullable &) = delete;
    auto operator=(const UniqueNullable &) -> UniqueNullable & = delete;

    explicit UniqueNullable(T resource):
          m_resource {resource} {}

    UniqueNullable(UniqueNullable &&rhs) noexcept
    {
        m_resource = std::exchange(rhs.m_resource, T {});
    }

    [[nodiscard]] auto is_valid() const -> bool
    {
        return m_resource;
    }

    auto reset() -> T
    {
        return std::exchange(m_resource, T {});;
    }

    auto operator=(UniqueNullable &&rhs) noexcept -> UniqueNullable &
    {
        m_resource = rhs.reset();
        return *this;
    }

    auto operator->() noexcept -> T&
    {
        return m_resource;
    }

    auto operator->() const noexcept -> const T&
    {
        return m_resource;
    }

    auto operator*() noexcept -> T&
    {
        return m_resource;
    }

    auto operator*() const noexcept -> const T&
    {
        return m_resource;
    }

private:
    T m_resource {};
};

class ReferenceCount final {
public:

    class Token final {
    public:
        explicit Token(std::atomic<unsigned> &count):
              m_count {&count}
        {
            count.fetch_add(1);
        }

        ~Token()
        {
            if (m_count.is_valid())
                m_count->fetch_sub(1);
        }

        Token(Token&&) = default;
        auto operator=(Token&&) -> Token& = default;

    private:
        UniqueNullable<std::atomic<unsigned>*> m_count;
    };

    ReferenceCount() = default;
    ~ReferenceCount() = default;

    [[nodiscard]] auto count() const -> Size
    {
        return m_count;
    }

    auto increment() -> Token
    {
        return Token {m_count};
    }

private:
    std::atomic<unsigned> m_count {};
};

} // cco::utils

#endif // CCO_UTILS_TYPES_H
