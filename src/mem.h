// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_MEM_H
#define CALICODB_MEM_H

#include "internal.h"
#include <new>

namespace calicodb
{

// Wrappers for system memory management routines
// Every heap-allocated object that CalicoDB uses must ultimately come from a call to either
// Mem::allocate() or Mem::reallocate(), and eventually be passed back to
// Mem::deallocate(). Differences between these routines and std::malloc(), std::realloc(),
// and std::free() are detailed here and below. The main difference is that a nonnull low-
// address pointer is returned from Mem::allocate(0) and Mem::reallocate(..., 0). This
// pointer must not be dereferenced, or passed to library functions that expect a valid
// pointer (like std::memcpy()). It can, however, be reallocated and/or freed. See
// https://yarchive.net/comp/linux/malloc_0.html for more details.
class Mem
{
public:
    // Calls the registered memory allocation function, which defaults to std::malloc()
    // Guarantees that allocate(0), the result of which is implementation-defined for
    // std::malloc(), returns a pointer to a zero-sized allocation with no side effects.
    // Source: https://en.cppreference.com/w/c/memory/malloc
    [[nodiscard]] static auto allocate(size_t len) -> void *;

    // Calls the registered memory reallocation function, which defaults to std::realloc()
    // Defines behavior for the following two cases, which are implementation-defined for
    // std::realloc():
    //
    //      Pattern                | Return                | Side effects
    //     ------------------------|-----------------------|--------------
    //      reallocate(nullptr, 0) | Zero-sized allocation | None
    //      reallocate(ptr, 0)     | Zero-sized allocation | ptr is freed
    //
    // Source: https://en.cppreference.com/w/c/memory/realloc
    [[nodiscard]] static auto reallocate(void *ptr, size_t len) -> void *;

    // Call the registered memory deallocation function, which defaults to std::free()
    static auto deallocate(void *ptr) -> void;

    template <class Object, class... Args>
    [[nodiscard]] static auto new_object(Args &&...args) -> Object *
    {
        Object *object;
        // NOTE: This probably won't work for types that require a stricter alignment than
        //       alignof(uint64_t).
        if (auto *storage = allocate(sizeof(Object))) {
            object = new (storage) Object(forward<Args &&>(args)...);
        } else {
            object = nullptr;
        }
        return object;
    }

    template <class Object>
    static auto delete_object(Object *ptr) -> void
    {
        if (ptr) {
            ptr->~Object();
            deallocate(ptr);
        }
    }
};

// Base class for objects that may need to be freed by the user
// Allows "delete ptr" to be used for cleanup.
struct HeapObject {
    explicit HeapObject();
    virtual ~HeapObject();

    static auto operator new(size_t size) -> void *
    {
        return operator new(size, std::nothrow);
    }

    static auto operator new(size_t size, const std::nothrow_t &) noexcept -> void *
    {
        return Mem::allocate(size);
    }

    static auto operator delete(void *ptr) -> void
    {
        Mem::deallocate(ptr);
    }
};

} // namespace calicodb

#endif // CALICODB_MEM_H
