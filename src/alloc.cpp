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

auto Alloc::alloc(size_t len) -> void *
{
    CALICODB_EXPECT_NE(len, 0);

    if (s_hook && s_hook(s_hook_arg)) {
        return nullptr;
    }
    return std::malloc(len);
}

auto Alloc::realloc(void *ptr, size_t len) -> void *
{
    CALICODB_EXPECT_NE(len, 0);

    if (s_hook && s_hook(s_hook_arg)) {
        return nullptr;
    }
    return std::realloc(ptr, len);
}

auto Alloc::free(void *ptr) -> void
{
    std::free(ptr);
}

} // namespace calicodb