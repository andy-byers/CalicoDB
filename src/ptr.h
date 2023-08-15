// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_PTR_H
#define CALICODB_PTR_H

#include "alloc.h"
#include <memory>
#include <utility>

namespace calicodb
{

template <class Object>
class PtrWrapper
{
    Object *m_ptr;

public:
    explicit PtrWrapper(Object *ptr = nullptr)
        : m_ptr(ptr)
    {
    }

    PtrWrapper(const PtrWrapper<Object> &) = delete;
    auto operator=(const PtrWrapper<Object> &) -> PtrWrapper<Object> & = delete;

    PtrWrapper(PtrWrapper<Object> &&rhs) noexcept
        : m_ptr(rhs.release())
    {
    }

    auto operator=(PtrWrapper<Object> &&rhs) noexcept -> PtrWrapper<Object> & = default;

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

    [[nodiscard]] auto is_valid() const -> bool
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

    auto release() -> Object *
    {
        return std::exchange(m_ptr, nullptr);
    }
};

template <class Object>
class UserPtr final : public PtrWrapper<Object>
{
public:
    explicit UserPtr(Object *ptr = nullptr)
        : PtrWrapper<Object>(ptr)
    {
    }

    ~UserPtr()
    {
        // This type of object is as part of the public API. It must be disposed-of using
        // operator delete(), since the Alloc interface is not exposed.
        delete this->get();
    }

    UserPtr(UserPtr<Object> &&) noexcept = default;
    auto operator=(UserPtr<Object> &&rhs) noexcept -> UserPtr<Object> &
    {
        if (this != &rhs) {
            reset(rhs.get());
            rhs.release();
        }
        return *this;
    }

    auto reset(Object *ptr = nullptr) -> void
    {
        delete this->get();
        std::exchange(this->ref(), ptr);
    }
};

template <class Object>
class ObjectPtr final : public PtrWrapper<Object>
{
public:
    explicit ObjectPtr(Object *ptr = nullptr)
        : PtrWrapper<Object>(ptr)
    {
    }

    ~ObjectPtr()
    {
        Alloc::delete_object(this->get());
    }

    ObjectPtr(ObjectPtr<Object> &&) noexcept = default;
    auto operator=(ObjectPtr<Object> &&rhs) noexcept -> ObjectPtr<Object> &
    {
        if (this != &rhs) {
            reset(rhs.get());
            rhs.release();
        }
        return *this;
    }

    auto reset(Object *ptr = nullptr) -> void
    {
        Alloc::delete_object(this->get());
        std::exchange(this->ref(), ptr);
    }
};

class StringPtr final : public PtrWrapper<char>
{
    mutable size_t m_len;

public:
    explicit StringPtr(char *ptr = nullptr, size_t len = 0)
        : PtrWrapper<char>(ptr),
          m_len(len ? len : std::strlen(ptr ? ptr : ""))
    {
    }

    ~StringPtr()
    {
        Alloc::free(this->get());
    }

    StringPtr(StringPtr &&) noexcept = default;
    auto operator=(StringPtr &&rhs) noexcept -> StringPtr &
    {
        if (this != &rhs) {
            reset(rhs.get());
            rhs.release();
        }
        return *this;
    }

    [[nodiscard]] auto len() const -> size_t
    {
        if (m_len == 0 && get()) {
            m_len = std::strlen(get());
        }
        return m_len;
    }

    auto reset(char *ptr = nullptr, size_t len = 0) -> void
    {
        Alloc::free(this->get());
        std::exchange(this->ref(), ptr);
        m_len = len;
    }

    auto resize(size_t len) -> void
    {
        if (auto *ptr = Alloc::realloc_string(get(), len + 1)) {
            release(); // Don't free the old pointer.
            reset(ptr);
        }
    }
};

} // namespace calicodb

#endif // CALICODB_PTR_H
