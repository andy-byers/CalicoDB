// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_ALLOC_H
#define CALICODB_ALLOC_H

#include "calicodb/slice.h"

namespace calicodb
{

// Allocation routines for internal objects
// Every heap-allocated object that CalicoDB uses must ultimately come from a call to either
// Alloc::alloc() or Alloc::realloc(), and eventually be passed back to Alloc::free().
class Alloc
{
public:
    // posix_memalign() is called on POSIX, and it has pretty strict requirements on the
    // alignment. It must be an integer multiple of sizeof(void *) that is also a power-of-2.
    // Source: https://en.cppreference.com/w/cpp/memory/c/aligned_alloc
    static constexpr size_t kMinAlignment = sizeof(void *);

    // Called in alloc() and realloc(). If the result is nonzero, a nullptr is returned
    // immediately, before the system allocator is called. Used for injecting OOM errors.
    using Hook = int (*)();
    static auto set_hook(Hook hook) -> void;

    [[nodiscard]] static auto alloc(size_t len) -> void *;
    [[nodiscard]] static auto alloc(size_t len, size_t alignment) -> void *;
    // Note that realloc() doesn't accept an alignment parameter. This routine must not be
    // used on storage obtained using alloc(size_t).
    [[nodiscard]] static auto realloc(void *ptr, size_t len) -> void *;
    static auto free(void *ptr) -> void;

    template <class Object, class... Args>
    [[nodiscard]] static auto new_object(Args &&...args) -> Object *
    {
        Object *object = nullptr; // May be aligned more strictly than necessary.
        const auto alignment = alignof(Object) < kMinAlignment
                                   ? kMinAlignment
                                   : alignof(Object);
        auto *storage = alloc(sizeof(Object), alignment);
        if (storage) {
            object = new (storage) Object(std::forward<Args &&>(args)...);
        }
        return object;
    }

    template <class Object>
    static auto delete_object(Object *ptr) -> void
    {
        if (ptr) {
            ptr->~Object();
            free(ptr);
        }
    }

    static auto alloc_string(size_t len) -> char *
    {
        return static_cast<char *>(alloc(len));
    }

    static auto realloc_string(char *ptr, size_t len) -> char *
    {
        return static_cast<char *>(realloc(ptr, len));
    }

    [[nodiscard]] static auto to_string(const Slice &slice) -> char *;
    [[nodiscard]] static auto combine(const Slice &left, const Slice &right) -> char *;
};

} // namespace calicodb

#endif // CALICODB_ALLOC_H
