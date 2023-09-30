// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_UNIQUE_PTR_H
#define CALICODB_UNIQUE_PTR_H

#include "calicodb/db.h"
#include "internal.h"
#include "internal_string.h"
#include "mem.h"

namespace calicodb
{

struct ObjectDestructor {
    template <class Object>
    auto operator()(Object *ptr) const -> void
    {
        Mem::delete_object(ptr);
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
        Mem::deallocate(ptr);
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

    explicit operator bool() const
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
        // WARNING: This convenience function is meant to be called like static_constructor(ptr.ref()),
        //          where static_constructor(Object *&object_out) allocates an object of type Object on
        //          the heap and passes its address to the caller through object_out. Memory is sure to
        //          be leaked if m_ptr points to a valid allocation.
        CALICODB_EXPECT_EQ(m_ptr, nullptr);
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

template <class Object>
using ObjectPtr = UniquePtr<Object, ObjectDestructor>;
template <class Object>
using UserPtr = UniquePtr<Object, UserObjectDestructor>;

} // namespace calicodb

#endif // CALICODB_UNIQUE_PTR_H
