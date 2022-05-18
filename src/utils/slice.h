/*
* slice.h: Implementation of the slice data types and their associated free functions. A slice represents an unowned,
*          contiguous subset of the elements of an array: essentially a pointer and a length. This code was inspired
*          by the slice class found in LevelDB (1).
*
* (1) https://github.com/google/leveldb/blob/main/include/leveldb/slice.h
*/

#ifndef CUB_SLICE_H
#define CUB_SLICE_H

#include <cstring>
#include <string>
#include "assert.h"
#include "common.h"

namespace cub {

enum class ThreeWayComparison {
   LT = -1,
   EQ =  0,
   GT =  1,
};

namespace impl {

   template<class Pointer> class Slice {
   public:
       using UnqualifiedPointer = std::remove_const_t<Pointer>;
       using ConstPointer = std::add_const_t<UnqualifiedPointer>;
       using Value = std::remove_pointer_t<UnqualifiedPointer>;

       Slice() noexcept = default;

       // Allow this implicit conversion for convenience.
       template<class Q> Slice(Slice<Q> rhs) noexcept
           : Slice{rhs.data(), rhs.size()} {}

       template<class Q> Slice(Q data, Size size) noexcept
           : m_data{data}
             , m_size{size} {}

       auto operator[](Index index) const noexcept -> const Value&
       {
           CUB_EXPECT_LT(index, m_size);
           return m_data[index];
       }

       auto operator[](Index index) noexcept -> Value&
       {
           CUB_EXPECT_LT(index, m_size);
           return m_data[index];
       }

       [[nodiscard]] auto is_empty() const noexcept -> bool
       {
           return m_size == 0;
       }

       [[nodiscard]] auto size() const noexcept -> Size
       {
           return m_size;
       }

       [[nodiscard]] auto copy() const -> Slice
       {
           return *this;
       }

       [[nodiscard]] auto range(Index offset, Size size) const noexcept -> Slice
       {
           CUB_EXPECT_LE(size, m_size);
           CUB_EXPECT_LE(offset, m_size);
           CUB_EXPECT_LE(offset + size, m_size);
           return Slice{m_data + offset, size};
       }

       [[nodiscard]] auto range(Index offset) const noexcept -> Slice
       {
           CUB_EXPECT_GE(m_size, offset);
           return range(offset, m_size - offset);
       }

       [[nodiscard]] auto data(Index index = 0) const noexcept -> ConstPointer
       {
           CUB_EXPECT_LE(index, m_size);
           return m_data + index;
       }

       auto data(Index index = 0) noexcept -> UnqualifiedPointer
       {
           CUB_EXPECT_LE(index, m_size);
           return m_data + index;
       }

       auto clear() noexcept -> void
       {
           m_data = nullptr;
           m_size = 0;
       }

       auto advance(Size n = 1) noexcept -> Slice
       {
           CUB_EXPECT_LE(n, m_size);
           m_data += n;
           m_size -= n;
           return *this;
       }

       auto truncate(Size size) noexcept -> Slice
       {
           CUB_EXPECT_LE(size, m_size);
           m_size = size;
           return *this;
       }

   private:
       Pointer m_data{};
       Size m_size{};
   };

} // impl

using MutBytes = impl::Slice<Byte*>;
using RefBytes = impl::Slice<const Byte*>;

inline auto to_bytes(const std::string &data) noexcept -> RefBytes
{
   return {data.data(), data.size()};
}

inline auto to_bytes(std::string &data) noexcept -> MutBytes
{
   return {data.data(), data.size()};
}

inline auto to_string(RefBytes data) -> std::string
{
   return {data.data(), data.size()};
}

inline auto compare_three_way(RefBytes lhs, RefBytes rhs) noexcept -> ThreeWayComparison
{
   const auto min_length{lhs.size() < rhs.size() ? lhs.size() : rhs.size()};
   auto r{std::memcmp(lhs.data(), rhs.data(), min_length)};
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

inline auto operator<(RefBytes lhs,  RefBytes rhs) noexcept -> bool
{
   return compare_three_way(lhs, rhs) == ThreeWayComparison::LT;
}

inline auto operator==(RefBytes lhs,  RefBytes rhs) noexcept -> bool
{
   return compare_three_way(lhs, rhs) == ThreeWayComparison::EQ;
}

inline auto operator!=(RefBytes lhs,  RefBytes rhs) noexcept -> bool
{
   return compare_three_way(lhs, rhs) == ThreeWayComparison::EQ;
}

inline auto mem_copy(MutBytes dst, RefBytes src, size_t n) noexcept -> void*
{
    CUB_EXPECT_LE(n, src.size());
    CUB_EXPECT_LE(n, dst.size());
    return std::memcpy(dst.data(), src.data(), n);
}

inline auto mem_copy(MutBytes dst, RefBytes src) noexcept -> void*
{
    CUB_EXPECT_EQ(src.size(), dst.size());
    return mem_copy(dst, src, src.size());
}

inline auto mem_clear(MutBytes mem, size_t n) noexcept -> void*
{
    CUB_EXPECT_LE(n, mem.size());
    return std::memset(mem.data(), 0, n);
}

inline auto mem_clear(MutBytes mem) noexcept -> void*
{
    return mem_clear(mem, mem.size());
}

} // cub

#endif // CUB_SLICE_H
