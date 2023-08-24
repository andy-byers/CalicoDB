// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "alloc.h"
#include "utils.h"
#include <limits>
#include <mutex>

namespace calicodb
{

// Prefix each allocation with its size, stored as an 8-byte unsigned integer.
using Header = uint64_t;

static constexpr auto kMaxLimit =
    std::numeric_limits<size_t>::max() - kMaxAllocation;

static struct Allocator {
    std::mutex mutex;
    Alloc::Methods methods = Alloc::kDefaultMethods;
    Alloc::Hook hook = nullptr;
    void *hook_arg = nullptr;
    uint64_t limit = kMaxLimit;
    uint64_t bytes_used = 0;
} s_alloc;

auto Alloc::bytes_used() -> size_t
{
    std::lock_guard lock(s_alloc.mutex);
    return s_alloc.bytes_used;
}

auto Alloc::set_hook(Hook fn, void *arg) -> void
{
    std::lock_guard lock(s_alloc.mutex);
    s_alloc.hook = fn;
    s_alloc.hook_arg = arg;
}

auto Alloc::set_limit(size_t limit) -> int
{
    std::lock_guard lock(s_alloc.mutex);
    if (s_alloc.bytes_used > limit) {
        return -1;
    }
    s_alloc.limit = limit ? limit : kMaxLimit;
    return 0;
}

auto Alloc::set_methods(const Methods &methods) -> int
{
    std::lock_guard lock(s_alloc.mutex);
    if (s_alloc.bytes_used) {
        return -1;
    }
    s_alloc.methods = methods;
    return 0;
}

static auto size_of_alloc(size_t size) -> size_t
{
    return size + sizeof(Header);
}

static auto size_of_alloc(void *ptr) -> size_t
{
    return size_of_alloc(static_cast<const Header *>(ptr)[-1]);
}

#define ALLOCATION_HOOK                                       \
    do {                                                      \
        if (s_alloc.hook && s_alloc.hook(s_alloc.hook_arg)) { \
            return nullptr;                                   \
        }                                                     \
    } while (0)

auto Alloc::malloc(size_t size) -> void *
{
    if (size == 0 || size > kMaxAllocation) {
        return nullptr;
    }
    std::lock_guard lock(s_alloc.mutex);
    ALLOCATION_HOOK;

    const auto with_hdr = size_of_alloc(size);
    if (s_alloc.bytes_used + with_hdr > s_alloc.limit) {
        return nullptr;
    }
    auto *ptr = static_cast<Header *>(
        s_alloc.methods.malloc(with_hdr));
    if (ptr) {
        s_alloc.bytes_used += with_hdr;
        *ptr++ = size;
    }
    return ptr;
}

auto Alloc::realloc(void *old_ptr, size_t new_size) -> void *
{
    if (old_ptr == nullptr) {
        return malloc(new_size);
    } else if (new_size == 0) {
        free(old_ptr);
        return nullptr;
    } else if (new_size > kMaxAllocation) {
        return nullptr;
    }
    std::lock_guard lock(s_alloc.mutex);
    ALLOCATION_HOOK;

    const auto old_with_hdr = size_of_alloc(old_ptr);
    const auto new_with_hdr = size_of_alloc(new_size);
    CALICODB_EXPECT_GT(old_with_hdr, sizeof(Header));
    CALICODB_EXPECT_GE(s_alloc.bytes_used, old_with_hdr);
    if (s_alloc.bytes_used - old_with_hdr + new_with_hdr > s_alloc.limit) {
        return nullptr;
    }

    auto *new_ptr = static_cast<Header *>(
        s_alloc.methods.realloc(static_cast<Header *>(old_ptr) - 1,
                                new_size + sizeof(Header)));
    if (new_ptr) {
        CALICODB_EXPECT_GE(s_alloc.bytes_used, old_with_hdr);
        s_alloc.bytes_used -= old_with_hdr;
        s_alloc.bytes_used += new_with_hdr;
        *new_ptr++ = new_size;
    }
    return new_ptr;
}

#undef ALLOCATION_HOOK

auto Alloc::free(void *ptr) -> void
{
    if (ptr) {
        std::lock_guard lock(s_alloc.mutex);
        CALICODB_EXPECT_GT(size_of_alloc(ptr), sizeof(Header));
        CALICODB_EXPECT_GE(s_alloc.bytes_used, size_of_alloc(ptr));
        s_alloc.bytes_used -= size_of_alloc(ptr);
        s_alloc.methods.free(static_cast<Header *>(ptr) - 1);
    }
}

HeapObject::HeapObject() = default;

HeapObject::~HeapObject() = default;

} // namespace calicodb