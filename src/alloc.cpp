// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "alloc.h"
#include "utils.h"

namespace calicodb
{

static Alloc::Hook s_hook = nullptr;

auto Alloc::set_hook(Hook hook) -> void
{
    s_hook = hook;
}

auto Alloc::alloc(size_t len) -> void *
{
    CALICODB_EXPECT_NE(len, 0);

    if (s_hook && s_hook()) {
        return nullptr;
    }
    return std::malloc(len);
}

auto Alloc::alloc(size_t len, size_t alignment) -> void *
{
    CALICODB_EXPECT_GT(len, 0);
    CALICODB_EXPECT_EQ(len % alignment, 0);
    CALICODB_EXPECT_EQ(alignment % sizeof(void *), 0);
    CALICODB_EXPECT_EQ(alignment & (alignment - 1), 0);
    CALICODB_EXPECT_GT(alignment, 0);

    if (s_hook && s_hook()) {
        return nullptr;
    }
    return std::aligned_alloc(alignment, len);
}

auto Alloc::realloc(void *ptr, size_t len) -> void *
{
    CALICODB_EXPECT_NE(len, 0);

    if (s_hook && s_hook()) {
        return nullptr;
    }
    return std::realloc(ptr, len);
}

auto Alloc::free(void *ptr) -> void
{
    std::free(ptr);
}

auto Alloc::to_string(const Slice &slice) -> char *
{
    // `slice` is assumed to not include a '\0' at the end.
    auto *str = alloc_string(slice.size() + 1);
    if (str) {
        std::memcpy(str, slice.data(), slice.size());
        str[slice.size()] = '\0';
    }
    return str;
}

auto Alloc::combine(const Slice &left, const Slice &right) -> char *
{
    auto *str = alloc_string(left.size() + right.size() + 1);
    if (str) {
        std::memcpy(str, left.data(), left.size());
        std::memcpy(str + left.size(), right.data(), right.size());
        str[left.size() + right.size()] = '\0';
    }
    return str;
}

} // namespace calicodb