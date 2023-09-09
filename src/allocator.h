// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_ALLOCATOR_H
#define CALICODB_ALLOCATOR_H

#include "mem.h"

#ifndef CALICODB_DEFAULT_MALLOC
#define CALICODB_DEFAULT_MALLOC std::malloc
#endif // CALICODB_DEFAULT_MALLOC

#ifndef CALICODB_DEFAULT_REALLOC
#define CALICODB_DEFAULT_REALLOC std::realloc
#endif // CALICODB_DEFAULT_REALLOC

#ifndef CALICODB_DEFAULT_FREE
#define CALICODB_DEFAULT_FREE std::free
#endif // CALICODB_DEFAULT_FREE

namespace calicodb
{

class DefaultAllocator
{
public:
    static auto methods() -> Mem::Methods;
};

// NOTE: Member functions are not thread-safe.
class DebugAllocator
{
public:
    static auto methods() -> Mem::Methods;

    static auto set_limit(size_t limit) -> size_t;

    // Allocation hook for testing
    using Hook = int (*)(void *);

    // Set a callback that is called in malloc() and realloc() with the provided `arg`.
    // If the result is nonzero, a nullptr is returned immediately, before the actual
    // allocation routine is called. Used for injecting random errors during testing.
    static auto set_hook(Hook hook, void *arg) -> void;

    // Get the total number of bytes allocated through malloc() and realloc() that have
    // not yet been passed to free()
    static auto bytes_used() -> size_t;

    static auto size_of(void *ptr) -> size_t;
};

} // namespace calicodb

#endif // CALICODB_ALLOCATOR_H
