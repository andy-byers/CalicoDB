#ifndef CALICO_UTILS_TYPES_H
#define CALICO_UTILS_TYPES_H

#include "utils.h"

namespace calico {

template<class Index>
struct IndexHash {

    [[nodiscard]]
    auto operator()(const Index &index) const -> std::size_t
    {
        return static_cast<std::size_t>(index.value);
    }
};

template<class T>
struct NullableId {

    [[nodiscard]]
    static constexpr auto null() noexcept -> T
    {
        return T {0};
    }

    [[nodiscard]]
    constexpr auto is_null() const noexcept -> bool
    {
        return static_cast<const T&>(*this).value == null().value;
    }

    [[nodiscard]]
    constexpr auto as_index() const noexcept -> Size
    {
        const auto &t = static_cast<const T&>(*this);
        CALICO_EXPECT_NE(t, null());
        return t.value - 1U;
    }

    [[nodiscard]]
    static constexpr auto from_index(Size t) noexcept -> T
    {
        return T {t + 1};
    }
};

template<class T1>
struct EqualityComparableTraits {

    template<class T2>
    auto operator==(const T2 &t) const noexcept -> bool
    {
        return static_cast<const T1&>(*this).value == T1 {t}.value;
    }

    template<class T2>
    auto operator!=(const T2 &t) const noexcept -> bool
    {
        return static_cast<const T1&>(*this).value != T1 {t}.value;
    }
};

template<class T1>
struct OrderableTraits {

    template<class T2>
    auto operator<(const T2 &t) const noexcept -> bool
    {
        return static_cast<const T1&>(*this).value < T1 {t}.value;
    }

    template<class T2>
    auto operator<=(const T2 &t) const noexcept -> bool
    {
        return static_cast<const T1&>(*this).value <= T1 {t}.value;
    }

    template<class T2>
    auto operator>(const T2 &t) const noexcept -> bool
    {
        return static_cast<const T1&>(*this).value > T1 {t}.value;
    }

    template<class T2>
    auto operator>=(const T2 &t) const noexcept -> bool
    {
        return static_cast<const T1&>(*this).value >= T1 {t}.value;
    }
};

template<class T>
struct IncrementableTraits { // TODO: Removed postfix increment. It's confusing to implement with CRTP!

    auto operator++() -> T&
    {
        static_cast<T&>(*this).value++;
        return static_cast<T&>(*this);
    }
};

struct PageId
    : public NullableId<PageId>,
      public EqualityComparableTraits<PageId>
{
    using Type = std::uint64_t;
    using Hash = IndexHash<PageId>;

    constexpr PageId() noexcept = default;

    template<class T>
    constexpr explicit PageId(T t) noexcept
        : value {std::uint64_t(t)}
    {}

    [[nodiscard]]
    static constexpr auto root() noexcept -> PageId
    {
        return PageId {1};
    }

    [[nodiscard]]
    constexpr auto is_root() const noexcept -> bool
    {
        return value == root().value;
    }

    std::uint64_t value {};
};

struct SequenceId
    : public NullableId<SequenceId>,
      public EqualityComparableTraits<SequenceId>,
      public OrderableTraits<SequenceId>
{
    using Type = std::uint64_t;
    using Hash = IndexHash<SequenceId>;

    constexpr SequenceId() noexcept = default;

    template<class U>
    constexpr explicit SequenceId(U u) noexcept
        : value {std::uint64_t(u)}
    {}

    auto operator++() -> SequenceId&
    {
        value++;
        return *this;
    }

    auto operator++(int) -> SequenceId
    {
        const auto temp = *this;
        ++(*this);
        return temp;
    }

    [[nodiscard]]
    static constexpr auto base() noexcept -> SequenceId
    {
        return SequenceId {1};
    }

    [[nodiscard]]
    constexpr auto is_base() const noexcept -> bool
    {
        return value == base().value;
    }

    std::uint64_t value {};
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
