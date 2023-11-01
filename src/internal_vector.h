// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_INTERNAL_VECTOR_H
#define CALICODB_INTERNAL_VECTOR_H

#include "mem.h"

namespace calicodb
{

template <class T>
class Vector final
{
public:
    struct RawParts {
        T *data;
        size_t size;
        size_t capacity;
    };
    static auto from_raw_parts(const RawParts &parts) -> Vector
    {
        Vector v;
        v.m_begin = parts.data;
        v.m_end = parts.data + parts.size;
        v.m_capacity = parts.data + parts.capacity;
        return v;
    }

    static auto into_raw_parts(Vector v) -> RawParts
    {
        RawParts parts;
        parts.data = v.data();
        parts.size = v.size();
        parts.capacity = v.capacity();
        v.m_begin = nullptr;
        v.m_end = nullptr;
        v.m_capacity = nullptr;
        return parts;
    }

    explicit Vector() = default;

    Vector(const Vector &rhs) noexcept = delete;
    auto operator=(const Vector &rhs) noexcept -> Vector & = delete;

    Vector(Vector &&rhs) noexcept
        : m_begin(exchange(rhs.m_begin, nullptr)),
          m_end(exchange(rhs.m_end, nullptr)),
          m_capacity(exchange(rhs.m_capacity, nullptr))
    {
    }

    ~Vector()
    {
        clear();
    }

    auto operator=(Vector &&rhs) noexcept -> Vector &
    {
        if (this != &rhs) {
            Mem::deallocate(m_begin);
            m_begin = exchange(rhs.m_begin, nullptr);
            m_end = exchange(rhs.m_end, nullptr);
            m_capacity = exchange(rhs.m_capacity, nullptr);
        }
        return *this;
    }

    auto operator[](size_t idx) -> T &
    {
        CALICODB_EXPECT_LT(idx, size());
        return m_begin[idx];
    }

    auto operator[](size_t idx) const -> const T &
    {
        CALICODB_EXPECT_LT(idx, size());
        return m_begin[idx];
    }

    [[nodiscard]] auto is_empty() const -> bool
    {
        return size() == 0;
    }

    [[nodiscard]] auto size() const -> size_t
    {
        return static_cast<size_t>(m_end - m_begin);
    }

    [[nodiscard]] auto data() -> T *
    {
        return m_begin;
    }

    [[nodiscard]] auto data() const -> const T *
    {
        return m_begin;
    }

    [[nodiscard]] auto front() -> T &
    {
        CALICODB_EXPECT_NE(m_begin, m_end);
        return m_begin[0];
    }

    [[nodiscard]] auto front() const -> const T &
    {
        CALICODB_EXPECT_NE(m_begin, m_end);
        return m_begin[0];
    }

    [[nodiscard]] auto back() -> T &
    {
        CALICODB_EXPECT_NE(m_begin, m_end);
        return m_end[-1];
    }

    [[nodiscard]] auto back() const -> const T &
    {
        CALICODB_EXPECT_NE(m_begin, m_end);
        return m_end[-1];
    }

    [[nodiscard]] auto begin() -> T *
    {
        return m_begin;
    }

    [[nodiscard]] auto begin() const -> const T *
    {
        return m_begin;
    }

    [[nodiscard]] auto end() -> T *
    {
        return m_end;
    }

    [[nodiscard]] auto end() const -> const T *
    {
        return m_end;
    }

    void clear()
    {
        shrink(0); // Call destructors if necessary
        Mem::deallocate(m_begin);
        m_begin = nullptr;
        m_end = nullptr;
        m_capacity = nullptr;
    }

    template <class... Args>
    [[nodiscard]] auto emplace_back(Args &&...args) -> int
    {
        if (reserve1()) {
            return -1;
        }
        new (m_end++) T(forward<Args>(args)...);
        return 0;
    }

    [[nodiscard]] auto push_back(T &&t) -> int
    {
        if (reserve1()) {
            return -1;
        }
        new (m_end++) T(move(t));
        return 0;
    }

    [[nodiscard]] auto push_back(const T &t) -> int
    {
        if (reserve1()) {
            return -1;
        }
        new (m_end++) T(t);
        return 0;
    }

    void pop_back()
    {
        CALICODB_EXPECT_LT(m_begin, m_end);
        --m_end;
        m_end->~T();
    }

    // [[nodiscard]] attribute was omitted intentionally. If there is no more memory available, the
    // caller will be notified again when attempting to add elements or resize.
    auto reserve(size_t target_capacity) -> int
    {
        auto n = capacity();
        if (n >= target_capacity) {
            return 0;
        }
        while (n < target_capacity) {
            n = (n + 1) * 2;
        }
        if (auto *ptr = static_cast<T *>(Mem::allocate(n * sizeof(T)))) {
            // Transfer existing elements over to the new storage.
            for (auto *p = ptr, *t = m_begin; t != m_end; ++t) {
                *p++ = move(*t);
            }
            m_end = ptr + size();
            m_capacity = ptr + n;
            Mem::deallocate(m_begin);
            m_begin = ptr;
            return 0;
        }
        return -1;
    }

    [[nodiscard]] auto resize(size_t target_size) -> int
    {
        if (target_size < size()) {
            shrink(target_size);
            return 0;
        }
        if (reserve(target_size)) {
            return -1;
        }
        const auto new_end = m_begin + target_size;
        for (auto *t = m_end; t != new_end; ++t) {
            new (t) T();
        }
        m_end = new_end;
        return 0;
    }

private:
    [[nodiscard]] auto reserve1() -> int
    {
        return reserve(size() + 1);
    }

    [[nodiscard]] auto capacity() const -> size_t
    {
        return static_cast<size_t>(m_capacity - m_begin);
    }

    void shrink(size_t target_size)
    {
        CALICODB_EXPECT_LE(target_size, size());
        const auto new_end = m_begin + target_size;
        for (auto *t = new_end; t != m_end; ++t) {
            t->~T();
        }
        m_end = new_end;
    }

    T *m_begin = nullptr;
    T *m_end = nullptr;
    T *m_capacity = nullptr;
};

} // namespace calicodb

#endif // CALICODB_INTERNAL_VECTOR_H
