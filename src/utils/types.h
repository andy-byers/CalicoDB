/**
 * References
 * (1) https://en.cppreference.com/w/cpp/utility/exchange
 */

#ifndef CCO_UTILS_TYPES_H
#define CCO_UTILS_TYPES_H

#include "calico/options.h"

namespace cco {

struct AlignedDeleter {

    explicit AlignedDeleter(std::align_val_t align)
        : alignment {align}
    {}

    auto operator()(Byte *ptr) const -> void
    {
        operator delete[](ptr, alignment);
    }

    std::align_val_t alignment;
};

using AlignedBuffer = std::unique_ptr<Byte[], AlignedDeleter>;

template<class T>
class UniqueNullable {
public:
    UniqueNullable() = delete;
    UniqueNullable(const UniqueNullable &) = delete;
    auto operator=(const UniqueNullable &) -> UniqueNullable & = delete;

    explicit UniqueNullable(T resource)
        : m_resource {resource}
    {}

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
        return std::exchange(m_resource, T {});
        ;
    }

    auto operator=(UniqueNullable &&rhs) noexcept -> UniqueNullable &
    {
        m_resource = rhs.reset();
        return *this;
    }

    auto operator->() noexcept -> T &
    {
        return m_resource;
    }

    auto operator->() const noexcept -> const T &
    {
        return m_resource;
    }

    auto operator*() noexcept -> T &
    {
        return m_resource;
    }

    auto operator*() const noexcept -> const T &
    {
        return m_resource;
    }

private:
    T m_resource {};
};

} // namespace cco

#endif // CCO_UTILS_TYPES_H
