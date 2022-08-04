/*
 * (1) https://github.com/google/leveldb/blob/main/include/leveldb/slice.h
 */

#ifndef CCO_BYTES_H
#define CCO_BYTES_H

#include <cassert>
#include <cstring>
#include "options.h"

namespace cco {

/**
 * Specifies an ordering between two entities.
 */
enum class ThreeWayComparison {
    LT = -1, ///< Less than
    EQ = 0, ///< Equal to
    GT = 1, ///< Greater than
};

namespace impl {

    /**
     * An internal class representing an unowned sequence of primitives.
     *
     * This class should only be used through the Bytes and BytesView aliases.
     *
     * @see Bytes
     * @see BytesView
     * @tparam Pointer The underlying pointer type.
     */
    template<class Pointer>
    class Slice {
    public:
        using Value = std::remove_pointer_t<Pointer>;

        /**
         * Create an empty slice.
         */
        Slice() noexcept = default;

        /**
         * Create a slice from another slice.
         *
         * This constructor exists to allow implicit conversions from Bytes to BytesView.
         *
         * @tparam Q The other slice pointer type.
         * @param rhs The other slice.
         */
        template<class Q>
        Slice(Slice<Q> rhs) noexcept:
              Slice {rhs.data(), rhs.size()} {}

        /**
         * Create a slice from a pointer and a length.
         *
         * @tparam Q The pointer type.
         * @param data A pointer to the start of a sequence.
         * @param size The length of the sequence.
         */
        template<class Q>
        Slice(Q data, Size size) noexcept:
              m_data {data},
              m_size {size} {}

        /**
         * Get a reference to the element at a specified index.
         *
         * @param index The index of the element.
         * @return A const reference to the element.
         */
        auto operator[](Index index) const noexcept -> const Value&
        {
            assert(index < m_size);
            return m_data[index];
        }

        /**
         * Get a reference to the element at a specified index.
         *
         * @param index The index of the element.
         * @return A reference to the element.
         */
        auto operator[](Index index) noexcept -> Value&
        {
            assert(index < m_size);
            return m_data[index];
        }

        /**
         * Determine if the slice is empty.
         *
         * @return True if the slice has zero length, false otherwise.
         */
        [[nodiscard]]
        auto is_empty() const noexcept -> bool
        {
            return m_size == 0;
        }

        /**
         * Get the length of the slice.
         *
         * @return The length in elements.
         */
        [[nodiscard]]
        auto size() const noexcept -> Size
        {
            return m_size;
        }

        /**
         * Get a copy of the slice.
         *
         * @return A copy of the slice.
         */
        [[nodiscard]]
        auto copy() const -> Slice
        {
            return *this;
        }

        /**
         * Create another slice out of a sub-section of this slice.
         *
         * @param offset The offset of the view in elements.
         * @param size The size of the view in elements.
         * @return The new slice.
         */
        [[nodiscard]]
        auto range(Index offset, Size size) const noexcept -> Slice
        {
            assert(size <= m_size);
            assert(offset <= m_size);
            assert(offset + size <= m_size);
            return Slice {m_data + offset, size};
        }

        /**
         * Create another slice out of a sub-section of this slice.
         *
         * @param offset The offset of the view in elements.
         * @return The new slice, spanning from the given offset to the end.
         */
        [[nodiscard]]
        auto range(Index offset) const noexcept -> Slice
        {
            assert(m_size >= offset);
            return range(offset, m_size - offset);
        }

        /**
         * Get the underlying pointer.
         *
         * @return The underlying pointer.
         */
        [[nodiscard]]
        auto data() const noexcept -> Pointer
        {
            return m_data;
        }

        /**
         * Get the underlying pointer.
         *
         * @return The underlying pointer.
         */
        [[nodiscard]]
        auto data() noexcept -> Pointer
        {
            return m_data;
        }

        /**
         * Invalidate the slice.
         */
        auto clear() noexcept -> void
        {
            m_data = nullptr;
            m_size = 0;
        }

        /**
         * Move the beginning of the slice forward.
         *
         * @param n The number of elements to advance by.
         * @return A copy of this slice for chaining.
         */
        auto advance(Size n = 1) noexcept -> Slice&
        {
            assert(n <= m_size);
            m_data += n;
            m_size -= n;
            return *this;
        }

        /**
         * Reduce the length of the slice.
         *
         * @param n The number of elements to reduce the length by.
         * @return A copy of this slice for chaining.
         */
        auto truncate(Size size) noexcept -> Slice&
        {
            assert(size <= m_size);
            m_size = size;
            return *this;
        }

        [[nodiscard]]
        auto starts_with(Slice<const Value*> rhs) const noexcept -> bool
        {
            if (rhs.size() > m_size)
                return false;
            return std::memcmp(m_data, rhs.data(), rhs.size()) == 0;
        }

    private:
        Pointer m_data {}; ///< Pointer to the beginning of the data.
        Size m_size {}; ///< Number of elements in the slice.
    };

} // impl

/**
 * Represents an unowned, mutable sequence of bytes.
 */
using Bytes = impl::Slice<Byte*>;

/**
 * Represents an unowned, immutable sequence of bytes.
 */
using BytesView = impl::Slice<const Byte*>;

/**
 * Create an immutable slice from a string.
 *
 * @param data The string.
 * @return The resulting immutable slice.
 */
inline auto stob(const std::string &data) noexcept -> BytesView
{
   return {data.data(), data.size()};
}

/**
 * Create a mutable slice from a string.
 *
 * @param data The string.
 * @return The resulting mutable slice.
 */
inline auto stob(std::string &data) noexcept -> Bytes
{
   return {data.data(), data.size()};
}

/**
 * Create an immutable slice from a C-style string.
 *
 * @param data The C-style string.
 * @return The resulting immutable slice.
 */
inline auto stob(const char *data) noexcept -> BytesView
{
    return {data, std::strlen(data)};
}

/**
 * Create a string out of a slice.
 *
 * @param data The slice.
 * @return The resulting string.
 */
inline auto btos(BytesView data) -> std::string
{
   return {data.data(), data.size()};
}

/**
 * Determine if one slice is less than, equal to, or greater than, another slice.
 *
 * @param lhs The first slice.
 * @param rhs The second slice.
 * @return An enum value representing the order between the two slices.
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

} // cco

inline auto operator<(cco::BytesView lhs, cco::BytesView rhs) noexcept -> bool
{
    return cco::compare_three_way(lhs, rhs) == cco::ThreeWayComparison::LT;
}

inline auto operator<=(cco::BytesView lhs, cco::BytesView rhs) noexcept -> bool
{
    return cco::compare_three_way(lhs, rhs) != cco::ThreeWayComparison::GT;
}

inline auto operator>(cco::BytesView lhs, cco::BytesView rhs) noexcept -> bool
{
    return cco::compare_three_way(lhs, rhs) == cco::ThreeWayComparison::GT;
}

inline auto operator>=(cco::BytesView lhs, cco::BytesView rhs) noexcept -> bool
{
    return cco::compare_three_way(lhs, rhs) != cco::ThreeWayComparison::LT;
}

inline auto operator==(cco::BytesView lhs, cco::BytesView rhs) noexcept -> bool
{
    return cco::compare_three_way(lhs, rhs) == cco::ThreeWayComparison::EQ;
}

inline auto operator!=(cco::BytesView lhs, cco::BytesView rhs) noexcept -> bool
{
    return !(lhs == rhs);
}

#endif // CCO_BYTES_H
