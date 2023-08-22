// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "alloc.h"
#include "utils.h"

namespace calicodb
{

static Alloc::Hook s_hook = nullptr;
static void *s_hook_arg = nullptr;

auto Alloc::set_hook(Hook hook, void *arg) -> void
{
    s_hook = hook;
    s_hook_arg = arg;
}

#define ALLOCATION_HOOK                     \
    do {                                    \
        if (s_hook && s_hook(s_hook_arg)) { \
            return nullptr;                 \
        }                                   \
    } while (0)

auto Alloc::calloc(size_t len, size_t size) -> void *
{
    CALICODB_EXPECT_NE(len | size, 0);
    ALLOCATION_HOOK;
    return std::calloc(len, size);
}

auto Alloc::malloc(size_t size) -> void *
{
    CALICODB_EXPECT_NE(size, 0);
    ALLOCATION_HOOK;
    return std::malloc(size);
}

auto Alloc::realloc(void *ptr, size_t size) -> void *
{
    CALICODB_EXPECT_NE(size, 0);
    ALLOCATION_HOOK;
    return std::realloc(ptr, size);
}

#undef ALLOCATION_HOOK

auto Alloc::free(void *ptr) -> void
{
    std::free(ptr);
}

} // namespace calicodb