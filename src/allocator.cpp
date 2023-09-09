// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "allocator.h"

namespace calicodb
{

namespace
{

constexpr auto kMaxLimit = SIZE_MAX - kMaxAllocation;
using DebugHeader = uint64_t;

struct {
    DebugAllocator::Hook hook = nullptr;
    void *hook_arg = nullptr;
    size_t limit = kMaxLimit;
    size_t bytes_used = 0;
} s_debug;

} // namespace

auto DefaultAllocator::methods() -> Mem::Methods
{
    return {
        CALICODB_DEFAULT_MALLOC,
        CALICODB_DEFAULT_REALLOC,
        CALICODB_DEFAULT_FREE,
    };
}

#define ALLOCATION_HOOK                                       \
    do {                                                      \
        if (s_debug.hook && s_debug.hook(s_debug.hook_arg)) { \
            return nullptr;                                   \
        }                                                     \
    } while (0)

auto debug_malloc(size_t size) -> void *
{
    CALICODB_EXPECT_NE(size, 0);
    const auto alloc_size = sizeof(DebugHeader) + size;
    if (s_debug.bytes_used + alloc_size > s_debug.limit) {
        return nullptr;
    }

    ALLOCATION_HOOK;

    auto *ptr = static_cast<DebugHeader *>(
        CALICODB_DEFAULT_MALLOC(alloc_size));
    if (ptr) {
        s_debug.bytes_used += alloc_size;
        *ptr++ = alloc_size;
    }
    return ptr;
}

auto debug_free(void *ptr) -> void
{
    CALICODB_EXPECT_NE(ptr, nullptr);
    const auto alloc_size = DebugAllocator::size_of(ptr);
    CALICODB_EXPECT_GT(alloc_size, sizeof(DebugHeader));
    CALICODB_EXPECT_LE(alloc_size, s_debug.bytes_used);

    // Fill the memory region with junk data. SQLite uses random bytes, which is probably more ideal,
    // but this should be good enough for now. This is intended to cause use-after-free bugs to be more
    // likely to result in crashes, rather than data corruption.
    std::memset(ptr, 0xFF, alloc_size - sizeof(DebugHeader));
    CALICODB_DEFAULT_FREE(static_cast<DebugHeader *>(ptr) - 1);
    s_debug.bytes_used -= alloc_size;
}

auto debug_realloc(void *old_ptr, size_t new_size) -> void *
{
    CALICODB_EXPECT_NE(new_size, 0);
    CALICODB_EXPECT_NE(old_ptr, nullptr);

    const auto new_alloc_size = sizeof(DebugHeader) + new_size;
    const auto old_alloc_size = DebugAllocator::size_of(old_ptr);
    CALICODB_EXPECT_GE(old_alloc_size, sizeof(DebugHeader));
    CALICODB_EXPECT_GE(s_debug.bytes_used, old_alloc_size);
    const auto grow = new_alloc_size > old_alloc_size
                          ? new_alloc_size - old_alloc_size
                          : 0;
    if (s_debug.bytes_used + grow > s_debug.limit) {
        return nullptr;
    }

    ALLOCATION_HOOK;

    // Call malloc() to get a new address. realloc() might resize the allocation inplace, but it
    // is undefined behavior to access the memory through the old pointer. If any code is doing
    // that, this makes it more likely to crash early rather than continue and  produce unexpected
    // results.
    auto *new_ptr = static_cast<DebugHeader *>(
        CALICODB_DEFAULT_MALLOC(new_alloc_size));
    if (new_ptr) {
        *new_ptr++ = new_alloc_size;

        // Copy the data over to the new allocation. Free the old allocation.
        const auto data_size = minval(old_alloc_size, new_alloc_size) - sizeof(DebugHeader);
        std::memcpy(new_ptr, old_ptr, data_size);
        debug_free(old_ptr);

        s_debug.bytes_used += new_alloc_size;
    }
    return new_ptr;
}

auto DebugAllocator::methods() -> Mem::Methods
{
    return {
        debug_malloc,
        debug_realloc,
        debug_free,
    };
}

auto DebugAllocator::set_limit(size_t limit) -> size_t
{
    limit = limit ? limit : kMaxLimit;
    if (s_debug.bytes_used <= limit) {
        return exchange(s_debug.limit, limit);
    }
    return 0;
}

auto DebugAllocator::set_hook(Hook hook, void *arg) -> void
{
    s_debug.hook = hook;
    s_debug.hook_arg = arg;
}

auto DebugAllocator::bytes_used() -> size_t
{
    return s_debug.bytes_used;
}

auto DebugAllocator::size_of(void *ptr) -> size_t
{
    return static_cast<DebugHeader *>(ptr)[-1];
}

} // namespace calicodb
