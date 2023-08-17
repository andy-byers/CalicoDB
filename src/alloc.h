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
    // Called in alloc() and realloc(). If the result is nonzero, a nullptr is returned
    // immediately, before the system allocator is called. Used for injecting OOM errors.
    using Hook = int (*)(void *);
    static auto set_hook(Hook hook, void *arg) -> void;

    [[nodiscard]] static auto calloc(size_t len, size_t size) -> void *;
    [[nodiscard]] static auto malloc(size_t len) -> void *;
    [[nodiscard]] static auto realloc(void *ptr, size_t len) -> void *;
    static auto free(void *ptr) -> void;

    template <class Object, class... Args>
    [[nodiscard]] static auto new_object(Args &&...args) -> Object *
    {
        Object *object;
        // NOTE: This probably won't work for types that require a stricter alignment than
        //       alignof(std::max_align_t).
        if (auto *storage = malloc(sizeof(Object))) {
            object = new (storage) Object(std::forward<Args &&>(args)...);
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
            free(ptr);
        }
    }
};

} // namespace calicodb

#endif // CALICODB_ALLOC_H
