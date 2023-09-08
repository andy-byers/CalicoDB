// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "alloc.h"
#include <atomic>

namespace calicodb
{

namespace
{

// Prefix each allocation with its size, stored as an 8-byte unsigned integer.
using Header = uint64_t;

constexpr auto kMaxLimit = SIZE_MAX - kMaxAllocation;

struct Allocator {
    Alloc::Methods methods = Alloc::kDefaultMethods;
    Alloc::Hook hook = nullptr;
    void *hook_arg = nullptr;
    uint64_t limit = kMaxLimit;
    std::atomic<uint64_t> bytes_used = 0;
} s_alloc;

auto size_of_alloc(size_t size) -> size_t
{
    return size + sizeof(Header);
}

auto size_of_alloc(void *ptr) -> size_t
{
    return size_of_alloc(static_cast<const Header *>(ptr)[-1]);
}

// Reserve `size` bytes of memory for allocation
// Ensures that the limit set by Alloc::set_limit() is respected.
auto reserve_memory(size_t size) -> int
{
    auto before = s_alloc.bytes_used.load(std::memory_order_relaxed);
    uint64_t after;
    do {
        after = before + size;
        if (after > s_alloc.limit) {
            return -1;
        }
    } while (!s_alloc.bytes_used.compare_exchange_weak(before, after));
    return 0;
}

// Give back `size bytes of memory
auto cancel_memory(size_t size) -> void
{
    [[maybe_unused]] const auto size_before = s_alloc.bytes_used.fetch_sub(size);
    CALICODB_EXPECT_GE(size_before, size);
}

} // namespace

#define ALLOCATION_HOOK                                       \
    do {                                                      \
        if (s_alloc.hook && s_alloc.hook(s_alloc.hook_arg)) { \
            return nullptr;                                   \
        }                                                     \
    } while (0)

auto Alloc::bytes_used() -> size_t
{
    return s_alloc.bytes_used.load(std::memory_order_relaxed);
}

auto Alloc::set_hook(Hook fn, void *arg) -> void
{
    s_alloc.hook = fn;
    s_alloc.hook_arg = arg;
}

auto Alloc::set_limit(size_t limit) -> int
{
    if (limit == 0) {
        s_alloc.limit = kMaxLimit;
    } else if (s_alloc.bytes_used > limit) {
        return -1;
    } else {
        s_alloc.limit = limit;
    }
    return 0;
}

auto Alloc::set_methods(const Methods &methods) -> int
{
    if (s_alloc.bytes_used) {
        return -1;
    }
    s_alloc.methods = methods;
    return 0;
}

auto Alloc::allocate(size_t size) -> void *
{
    if (size == 0) {
        return zero_size_ptr<void>();
    } else if (size > kMaxAllocation) {
        return nullptr;
    }
    ALLOCATION_HOOK;

    const auto alloc_size = size_of_alloc(size);
    if (reserve_memory(alloc_size)) {
        return nullptr;
    }
    auto *ptr = static_cast<Header *>(
        s_alloc.methods.malloc(alloc_size));
    if (ptr) {
        *ptr++ = size;
    } else {
        // Memory was reserved, but actual malloc() failed.
        cancel_memory(alloc_size);
    }
    return ptr;
}

auto Alloc::reallocate(void *old_ptr, size_t new_size) -> void *
{
    if (!old_ptr || old_ptr == zero_size_ptr<void>()) {
        return allocate(new_size);
    } else if (new_size == 0) {
        deallocate(old_ptr);
        return zero_size_ptr<void>();
    } else if (new_size > kMaxAllocation) {
        return nullptr;
    }
    ALLOCATION_HOOK;

    const auto old_alloc_size = size_of_alloc(old_ptr);
    const auto new_alloc_size = size_of_alloc(new_size);
    CALICODB_EXPECT_GT(old_alloc_size, sizeof(Header));
    CALICODB_EXPECT_GE(s_alloc.bytes_used.load(), old_alloc_size);
    const auto grow = new_alloc_size > old_alloc_size
                          ? new_alloc_size - old_alloc_size
                          : 0;
    const auto shrink = grow ? 0 : old_alloc_size - new_alloc_size;
    CALICODB_EXPECT_GE(s_alloc.bytes_used.load(), shrink);
    if (grow && reserve_memory(grow)) {
        return nullptr;
    }

    auto *new_ptr = static_cast<Header *>(
        s_alloc.methods.realloc(static_cast<Header *>(old_ptr) - 1,
                                new_size + sizeof(Header)));
    if (new_ptr) {
        *new_ptr++ = new_size;
    }

    if ((new_ptr == nullptr && grow) ||   // Reserved memory, but realloc() failed
        (new_ptr != nullptr && shrink)) { // Succeeded in shrinking the allocation
        cancel_memory(grow + shrink);
    }
    return new_ptr;
}

auto Alloc::deallocate(void *ptr) -> void
{
    if (ptr && ptr != zero_size_ptr<void>()) {
        CALICODB_EXPECT_GT(size_of_alloc(ptr), sizeof(Header));
        cancel_memory(size_of_alloc(ptr));
        s_alloc.methods.free(static_cast<Header *>(ptr) - 1);
    }
}

HeapObject::HeapObject() = default;

HeapObject::~HeapObject() = default;

} // namespace calicodb