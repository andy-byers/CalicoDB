#ifndef CALICO_UTILS_TYPES_H
#define CALICO_UTILS_TYPES_H

#include <utility>
#include <vector>
#include "utils.h"

namespace calico {

using size_t = std::size_t;

struct identifier {
    static constexpr size_t null_value {0};
    static constexpr size_t root_value {1};

    struct hash {
        auto operator()(const identifier &id) const -> size_t
        {
            return id.value;
        }
    };

    [[nodiscard]]
    static constexpr auto from_index(size_t index) noexcept -> identifier
    {
        return {index + 1};
    }

    [[nodiscard]]
    static constexpr auto null() noexcept -> identifier
    {
        return {null_value};
    }

    [[nodiscard]]
    static constexpr auto root() noexcept -> identifier
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

    constexpr auto operator<=>(const identifier &) const = default;

    size_t value {};
};

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

} // namespace calico

#endif // CALICO_UTILS_TYPES_H
