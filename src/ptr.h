// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_PTR_H
#define CALICODB_PTR_H

#include "alloc.h"
#include "utils.h"
#include <memory>
#include <utility>

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
        Alloc::free(ptr);
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
        : Destructor(std::forward<Dx>(destructor)),
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
          m_ptr(std::exchange(rhs.m_ptr, nullptr))
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
        return std::exchange(m_ptr, nullptr);
    }
};

class UniqueBuffer final
{
    UniquePtr<char> m_ptr;
    size_t m_len;

public:
    explicit UniqueBuffer()
        : m_len(0)
    {
    }

    explicit UniqueBuffer(char *ptr, size_t len)
        : m_ptr(ptr),
          m_len(len)
    {
        CALICODB_EXPECT_EQ(ptr == nullptr, len == 0);
    }

    UniqueBuffer(UniqueBuffer &&rhs) noexcept
        : m_ptr(std::move(rhs.m_ptr)),
          m_len(std::exchange(rhs.m_len, 0))
    {
    }

    auto operator=(UniqueBuffer &&rhs) noexcept -> UniqueBuffer &
    {
        if (this != &rhs) {
            m_ptr = std::move(rhs.m_ptr);
            m_len = std::exchange(rhs.m_len, 0);
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

    [[nodiscard]] auto ptr() -> char *
    {
        return m_ptr.get();
    }

    [[nodiscard]] auto ptr() const -> const char *
    {
        return m_ptr.get();
    }

    [[nodiscard]] auto ref() -> char *&
    {
        return m_ptr.ref();
    }

    auto reset(char *ptr, size_t len) -> void
    {
        CALICODB_EXPECT_EQ(ptr == nullptr, len == 0);
        m_ptr.reset(ptr);
        m_len = len;
    }

    auto release() -> char *
    {
        m_len = 0;
        return std::exchange(ref(), nullptr);
    }

    auto resize(size_t len) -> void
    {
        CALICODB_EXPECT_GT(len, 0); // Use reset(nullptr, 0) to free the buffer early.
        auto *ptr = static_cast<char *>(Alloc::realloc(m_ptr.get(), len));
        if (ptr) {
            m_ptr.release();
        } else {
            len = 0;
        }
        m_ptr.reset(ptr);
        m_len = len;
    }

    // NOTE: A null byte is added at the end of the buffer.
    static auto from_slice(const Slice &slice, const Slice &extra = "") -> UniqueBuffer
    {
        const auto len = slice.size() + extra.size();

        UniqueBuffer buf;
        buf.resize(len + 1);
        if (!buf.is_empty()) {
            std::memcpy(buf.ptr(), slice.data(), slice.size());
            std::memcpy(buf.ptr() + slice.size(), extra.data(), extra.size());
            buf.ptr()[len] = '\0';
        }
        return buf;
    }

    [[nodiscard]] auto as_slice() const -> Slice
    {
        return is_empty() ? "" : Slice(m_ptr.get(), m_len - 1);
    }
};

template <class Object>
using ObjectPtr = UniquePtr<Object, ObjectDestructor>;
template <class Object>
using UserPtr = UniquePtr<Object, UserObjectDestructor>;

} // namespace calicodb

#endif // CALICODB_PTR_H
