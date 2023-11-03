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

    T *m_data = nullptr;
    size_t m_size = 0;

public:
    struct RawParts {
        T *data;
        size_t size;
    };

    explicit Buffer() = default;

    explicit Buffer(const RawParts &parts)
        : m_data(parts.data),
          m_size(parts.size)
    {
        CALICODB_EXPECT_EQ(!parts.data, !parts.size);
    }

    ~Buffer()
    {
        Mem::deallocate(m_data);
    }

    Buffer(Buffer &&rhs) noexcept
        : m_data(exchange(rhs.m_data, nullptr)),
          m_size(exchange(rhs.m_size, 0U))
    {
    }

    auto operator=(Buffer &&rhs) noexcept -> Buffer &
    {
        if (this != &rhs) {
            Mem::deallocate(m_data);
            m_data = exchange(rhs.m_data, nullptr);
            m_size = exchange(rhs.m_size, 0U);
        }
        return *this;
    }

    auto operator[](size_t idx) -> T &
    {
        CALICODB_EXPECT_LT(idx, m_size);
        return m_data[idx];
    }

    auto operator[](size_t idx) const -> const T &
    {
        CALICODB_EXPECT_LT(idx, m_size);
        return m_data[idx];
    }

    [[nodiscard]] auto is_empty() const -> bool
    {
        return m_size == 0;
    }

    [[nodiscard]] auto size() const -> size_t
    {
        return m_size;
    }

    [[nodiscard]] auto data() -> T *
    {
        return m_data;
    }

    [[nodiscard]] auto data() const -> const T *
    {
        return m_data;
    }

    void reset()
    {
        Mem::deallocate(m_data);
        m_data = nullptr;
        m_size = 0;
    }

    void reset(const RawParts &parts)
    {
        CALICODB_EXPECT_EQ(!parts.data, !parts.size);
        Mem::deallocate(m_data);
        m_data = parts.data;
        m_size = parts.size;
    }

    auto release() && -> RawParts
    {
        return {
            exchange(m_data, nullptr),
            exchange(m_size, 0U),
        };
    }

    [[nodiscard]] auto resize(size_t size) -> int
    {
        auto *data = static_cast<T *>(Mem::reallocate(
            m_data, size * sizeof(T)));
        if (data || size == 0) {
            m_data = data;
            m_size = size;
            return 0;
        }
        return -1;
    }

    [[nodiscard]] auto realloc(size_t size) -> int
    {
        // Free the old allocation. This method should be chosen over resize() if the old buffer
        // contents are not important. Avoids an unnecessary copy if the allocator could not grow
        // the allocation inplace.
        reset();
        return resize(size);
    }
};

} // namespace calicodb

#endif // CALICODB_BUFFER_H
