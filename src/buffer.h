// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_BUFFER_H
#define CALICODB_BUFFER_H

#include "unique_ptr.h"
#include <type_traits>

namespace calicodb
{

template <class T>
class Buffer final
{
    static_assert(std::is_trivially_copyable_v<T>);

    UniquePtr<T> m_ptr;
    size_t m_len;

public:
    explicit Buffer()
        : m_len(0)
    {
    }

    explicit Buffer(T *ptr, size_t len)
        : m_ptr(ptr),
          m_len(len)
    {
        CALICODB_EXPECT_EQ(ptr == nullptr, len == 0);
    }

    Buffer(Buffer &&rhs) noexcept
        : m_ptr(move(rhs.m_ptr)),
          m_len(exchange(rhs.m_len, 0U))
    {
    }

    auto operator=(Buffer &&rhs) noexcept -> Buffer &
    {
        if (this != &rhs) {
            m_ptr = move(rhs.m_ptr);
            m_len = exchange(rhs.m_len, 0U);
        }
        return *this;
    }

    auto operator[](size_t idx) -> T &
    {
        CALICODB_EXPECT_LT(idx, m_len);
        return m_ptr.get()[idx];
    }

    auto operator[](size_t idx) const -> const T &
    {
        CALICODB_EXPECT_LT(idx, m_len);
        return m_ptr.get()[idx];
    }

    [[nodiscard]] auto is_empty() const -> bool
    {
        return m_len == 0;
    }

    [[nodiscard]] auto len() const -> size_t
    {
        return m_len;
    }

    [[nodiscard]] auto ptr() -> T *
    {
        return m_ptr.get();
    }

    [[nodiscard]] auto ptr() const -> const T *
    {
        return m_ptr.get();
    }

    [[nodiscard]] auto ref() -> T *&
    {
        return m_ptr.ref();
    }

    auto reset() -> void
    {
        m_ptr.reset(nullptr);
        m_len = 0;
    }

    auto reset(char *ptr, size_t len) -> void
    {
        CALICODB_EXPECT_NE(ptr, nullptr);
        CALICODB_EXPECT_NE(len, 0);
        m_ptr.reset(ptr);
        m_len = len;
    }

    auto release() -> T *
    {
        m_len = 0;
        return m_ptr.release();
    }

    [[nodiscard]] auto realloc(size_t len) -> int
    {
        auto *ptr = static_cast<T *>(Mem::reallocate(
            m_ptr.get(), len * sizeof(T)));
        if (ptr || len == 0) {
            m_ptr.release();
            m_ptr.reset(ptr);
            m_len = len;
            return 0;
        }
        return -1;
    }
};

} // namespace calicodb

#endif // CALICODB_BUFFER_H
