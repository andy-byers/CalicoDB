// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_PTR_H
#define CALICODB_PTR_H

#include "alloc.h"
#include "calicodb/db.h"
#include "calicodb/string.h"
#include "utils.h"

namespace calicodb
{

struct ObjectDestructor {
    template <class Object>
    auto operator()(Object *ptr) const -> void
    {
        Alloc::delete_object(ptr);
    }
};

struct UserObjectDestructor {
    template <class Object>
    auto operator()(Object *ptr) const -> void
    {
        delete ptr;
    }
};

struct DefaultDestructor {
    auto operator()(void *ptr) const -> void
    {
        Alloc::deallocate(ptr);
    }
};

template <class Object, class Destructor = DefaultDestructor>
class UniquePtr : private Destructor
{
    Object *m_ptr;

    auto destroy() const
    {
        Destructor::operator()(m_ptr);
    }

public:
    explicit UniquePtr(Object *ptr = nullptr)
        : m_ptr(ptr)
    {
    }

    template <class Dx>
    explicit UniquePtr(Object *ptr, Dx &&destructor)
        : Destructor(forward<Dx>(destructor)),
          m_ptr(ptr)
    {
    }

    ~UniquePtr()
    {
        destroy();
    }

    UniquePtr(const UniquePtr &rhs) noexcept = delete;
    auto operator=(const UniquePtr &rhs) noexcept -> UniquePtr & = delete;

    UniquePtr(UniquePtr &&rhs) noexcept
        : Destructor(rhs),
          m_ptr(exchange(rhs.m_ptr, nullptr))
    {
    }

    auto operator=(UniquePtr &&rhs) noexcept -> UniquePtr &
    {
        if (this != &rhs) {
            reset(rhs.release());
        }
        return *this;
    }

    auto operator*() -> Object &
    {
        return *m_ptr;
    }

    auto operator*() const -> const Object &
    {
        return *m_ptr;
    }

    auto operator->() -> Object *
    {
        return m_ptr;
    }

    auto operator->() const -> const Object *
    {
        return m_ptr;
    }

    operator bool() const
    {
        return m_ptr != nullptr;
    }

    [[nodiscard]] auto get() -> Object *
    {
        return m_ptr;
    }

    [[nodiscard]] auto get() const -> const Object *
    {
        return m_ptr;
    }

    [[nodiscard]] auto ref() -> Object *&
    {
        return m_ptr;
    }

    auto reset(Object *ptr = nullptr) -> void
    {
        destroy();
        m_ptr = ptr;
    }

    auto release() -> Object *
    {
        return exchange(m_ptr, nullptr);
    }
};

template <class T>
class UniqueBuffer final
{
    static_assert(std::is_trivially_copyable_v<T>);

    UniquePtr<T> m_ptr;
    size_t m_len;

public:
    explicit UniqueBuffer()
        : m_len(0)
    {
    }

    explicit UniqueBuffer(T *ptr, size_t len)
        : m_ptr(ptr),
          m_len(len)
    {
        CALICODB_EXPECT_EQ(ptr == nullptr, len == 0);
    }

    UniqueBuffer(UniqueBuffer &&rhs) noexcept
        : m_ptr(move(rhs.m_ptr)),
          m_len(exchange(rhs.m_len, 0U))
    {
    }

    auto operator=(UniqueBuffer &&rhs) noexcept -> UniqueBuffer &
    {
        if (this != &rhs) {
            m_ptr = move(rhs.m_ptr);
            m_len = exchange(rhs.m_len, 0U);
        }
        return *this;
    }

    [[nodiscard]] auto is_empty() const -> bool
    {
        // Emptiness only depends on the pointer. The size may be nonzero.
        return !m_ptr;
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
        return exchange(ref(), nullptr);
    }

    [[nodiscard]] auto realloc(size_t len) -> int
    {
        auto *ptr = static_cast<T *>(Alloc::reallocate(m_ptr.get(), len * sizeof(T)));
        if (ptr || len == 0) {
            m_ptr.release();
            m_ptr.reset(ptr);
            m_len = len;
            return 0;
        }
        return -1;
    }
};

template <class Object>
using ObjectPtr = UniquePtr<Object, ObjectDestructor>;
template <class Object>
using UserPtr = UniquePtr<Object, UserObjectDestructor>;

} // namespace calicodb

#endif // CALICODB_PTR_H
