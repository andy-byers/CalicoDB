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
    using Hook = int (*)(void *);
    using Malloc = void *(*)(size_t);
    using Realloc = void *(*)(void *, size_t);
    using Free = void (*)(void *);

    struct Methods {
        Malloc malloc;
        Realloc realloc;
        Free free;
    };

    static constexpr Methods kDefaultMethods = {
        std::malloc,
        std::realloc,
        std::free,
    };

    static auto set_methods(const Methods &methods) -> int;
    static auto set_limit(size_t limit) -> int;
    static auto bytes_used() -> size_t;

    // Set a callback that is called in alloc() and realloc() with the provided `arg`. If
    // the result is nonzero, a nullptr is returned immediately, before the actual allocation
    // routine is called. Used for injecting random errors during testing.
    static auto set_hook(Hook hook, void *arg) -> void;

    // Calls the registered memory allocation function, which defaults to std::malloc()
    // Guarantees that malloc(0), the result of which is implementation-defined for
    // std::malloc(), returns nullptr with no side effects.
    //
    // Source: https://en.cppreference.com/w/c/memory/malloc
    [[nodiscard]] static auto malloc(size_t len) -> void *;

    // Calls the registered memory reallocation function, which defaults to std::realloc()
    // Defines behavior for the following two cases, which are implementation-defined for
    // std::realloc():
    //
    //      Pattern             | Return  | Side effects
    //     ---------------------|---------|--------------
    //      realloc(nullptr, 0) | nullptr | None
    //      realloc(ptr, 0)     | nullptr | ptr is freed
    //
    // Source: https://en.cppreference.com/w/c/memory/realloc
    [[nodiscard]] static auto realloc(void *ptr, size_t len) -> void *;

    // Call the registered memory deallocation function, which defaults to std::free()
    static auto free(void *ptr) -> void;

    template <class Object, class... Args>
    [[nodiscard]] static auto new_object(Args &&...args) -> Object *
    {
        Object *object;
        // NOTE: This probably won't work for types that require a stricter alignment than
        //       alignof(uint64_t).
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
