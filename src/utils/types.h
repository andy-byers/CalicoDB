#ifndef CALICO_UTILS_TYPES_H
#define CALICO_UTILS_TYPES_H

#include <utility>
#include <vector>
#include "utils.h"

namespace Calico {

struct Id {
    static constexpr Size null_value {0};
    static constexpr Size root_value {1};

    struct Hash {
        auto operator()(const Id &id) const -> Size
        {
            return id.value;
        }
    };

    [[nodiscard]]
    static constexpr auto from_index(Size index) noexcept -> Id
    {
        return {index + 1};
    }

    [[nodiscard]]
    static constexpr auto null() noexcept -> Id
    {
        return {null_value};
    }

    [[nodiscard]]
    static constexpr auto root() noexcept -> Id
    {
        return {root_value};
    }

    [[nodiscard]]
    constexpr auto is_null() const noexcept -> bool
    {
        return value == null_value;
    }

    [[nodiscard]]
    constexpr auto is_root() const noexcept -> bool
    {
        return value == root_value;
    }

    [[nodiscard]]
    constexpr auto as_index() const noexcept -> Size
    {
        CALICO_EXPECT_NE(value, null().value);
        return value - 1;
    }

    Size value {};
};

inline auto operator<(Id lhs, Id rhs) -> bool
{
    return lhs.value < rhs.value;
}

inline auto operator>(Id lhs, Id rhs) -> bool
{
    return lhs.value > rhs.value;
}

inline auto operator<=(Id lhs, Id rhs) -> bool
{
    return lhs.value <= rhs.value;
}

inline auto operator>=(Id lhs, Id rhs) -> bool
{
    return lhs.value >= rhs.value;
}

inline auto operator==(Id lhs, Id rhs) -> bool
{
    return lhs.value == rhs.value;
}

inline auto operator!=(Id lhs, Id rhs) -> bool
{
    return lhs.value != rhs.value;
}

class AlignedBuffer {
public:
    AlignedBuffer(Size size, Size alignment)
        : m_data {
              new(std::align_val_t {alignment}, std::nothrow) Byte[size],
              Deleter {std::align_val_t {alignment}},
          }
    {
        CALICO_EXPECT_TRUE(is_power_of_two(alignment));
        CALICO_EXPECT_EQ(size % alignment, 0);
    }

    [[nodiscard]]
    auto get() -> Byte *
    {
        return m_data.get();
    }

    [[nodiscard]]
    auto get() const -> const Byte *
    {
        return m_data.get();
    }

private:
    struct Deleter {
        auto operator()(Byte *ptr) const -> void
        {
            operator delete[](ptr, alignment);
        }

        std::align_val_t alignment;
    };

    std::unique_ptr<Byte[], Deleter> m_data;
};

template<class T>
class UniqueNullable final {
public:
    using Type = T;

    ~UniqueNullable() = default;
    UniqueNullable() = delete;
    UniqueNullable(const UniqueNullable &) = delete;
    auto operator=(const UniqueNullable &) -> UniqueNullable & = delete;

    template<class Resource>
    explicit UniqueNullable(Resource resource)
        : m_resource {resource}
    {}

    UniqueNullable(UniqueNullable &&rhs) noexcept
    {
        m_resource = std::exchange(rhs.m_resource, T {});
    }

    auto operator=(UniqueNullable &&rhs) noexcept -> UniqueNullable &
    {
        m_resource = rhs.reset();
        return *this;
    }

    [[nodiscard]] auto is_valid() const -> bool
    {
        return m_resource;
    }

    auto reset() -> T
    {
        return std::exchange(m_resource, T {});
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

} // namespace Calico

#endif // CALICO_UTILS_TYPES_H
