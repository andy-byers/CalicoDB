/*
 * (1) https://github.com/google/leveldb/blob/main/include/leveldb/slice.h
 */

#ifndef CUB_BYTES_H
#define CUB_BYTES_H

#include <cstring>
#include <string>
#include "utils/assert.h"
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
           : Slice {rhs.data(), rhs.size()} {}

       template<class Q> Slice(Q data, Size size) noexcept
           : m_data {data}
           , m_size {size} {}

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

       [[nodiscard]] auto data() const noexcept -> ConstPointer
       {
           return m_data;
       }

       auto data() noexcept -> UnqualifiedPointer
       {
           return m_data;
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

using Bytes = impl::Slice<Byte*>;
using BytesView = impl::Slice<const Byte*>;

inline auto _b(const std::string &data) noexcept -> BytesView
{
   return {data.data(), data.size()};
}

inline auto _b(std::string &data) noexcept -> Bytes
{
   return {data.data(), data.size()};
}

inline auto _s(BytesView data) -> std::string
{
   return {data.data(), data.size()};
}

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

inline auto operator<(BytesView lhs, BytesView rhs) noexcept -> bool
{
   return compare_three_way(lhs, rhs) == ThreeWayComparison::LT;
}

inline auto operator==(BytesView lhs, BytesView rhs) noexcept -> bool
{
   return compare_three_way(lhs, rhs) == ThreeWayComparison::EQ;
}

inline auto operator!=(BytesView lhs, BytesView rhs) noexcept -> bool
{
   return compare_three_way(lhs, rhs) != ThreeWayComparison::EQ;
}

inline auto mem_copy(Bytes dst, BytesView src, size_t n) noexcept -> void*
{
    CUB_EXPECT_LE(n, src.size());
    CUB_EXPECT_LE(n, dst.size());
    return std::memcpy(dst.data(), src.data(), n);
}

inline auto mem_copy(Bytes dst, BytesView src) noexcept -> void*
{
    CUB_EXPECT_LE(src.size(), dst.size());
    return mem_copy(dst, src, src.size());
}

inline auto mem_clear(Bytes mem, size_t n) noexcept -> void*
{
    CUB_EXPECT_LE(n, mem.size());
    return std::memset(mem.data(), 0, n);
}

inline auto mem_clear(Bytes mem) noexcept -> void*
{
    return mem_clear(mem, mem.size());
}

inline auto mem_move(Bytes dst, BytesView src, Size n) noexcept -> void*
{
    CUB_EXPECT_LE(n, src.size());
    CUB_EXPECT_LE(n, dst.size());
    return std::memmove(dst.data(), src.data(), n);
}

inline auto mem_move(Bytes dst, BytesView src) noexcept -> void*
{
    CUB_EXPECT_LE(src.size(), dst.size());
    return mem_move(dst, src, src.size());
}

} // db

#endif // CUB_BYTES_H
