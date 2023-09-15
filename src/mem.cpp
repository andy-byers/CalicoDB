// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "mem.h"
#include "config_internal.h"
#include "port.h"

namespace calicodb
{

namespace
{

struct AllocatorState {
    port::Mutex mutex;
} s_state;

} // namespace

auto Mem::allocate(size_t size) -> void *
{
    if (size == 0 || size > kMaxAllocation) {
        return nullptr;
    }
    s_state.mutex.lock();
    auto *ptr = g_config.allocator.malloc(size);
    s_state.mutex.unlock();
    return ptr;
}

auto Mem::reallocate(void *old_ptr, size_t new_size) -> void *
{
    if (old_ptr == nullptr) {
        return allocate(new_size);
    } else if (new_size == 0) {
        deallocate(old_ptr);
        return nullptr;
    } else if (new_size > kMaxAllocation) {
        return nullptr;
    }

    s_state.mutex.lock();
    auto *new_ptr = g_config.allocator.realloc(old_ptr, new_size);
    s_state.mutex.unlock();
    return new_ptr;
}

auto Mem::deallocate(void *ptr) -> void
{
    if (ptr) {
        s_state.mutex.lock();
        g_config.allocator.free(ptr);
        s_state.mutex.unlock();
    }
}

HeapObject::HeapObject() = default;

HeapObject::~HeapObject() = default;

} // namespace calicodb