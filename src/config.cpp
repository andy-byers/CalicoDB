// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "config_internal.h"

namespace calicodb
{

namespace
{

constexpr AllocatorConfig kDefaultAllocatorConfig = {
    CALICODB_DEFAULT_MALLOC,
    CALICODB_DEFAULT_REALLOC,
    CALICODB_DEFAULT_FREE,
};

} // namespace

// Per-process configuration options.
Config g_config = {
    kDefaultAllocatorConfig,
};

// Defined in src/env_*.cpp
auto replace_syscall(const SyscallConfig &config) -> Status;
auto restore_syscall(const char *name) -> Status;

auto configure(ConfigTarget target, const void *value) -> Status
{
    switch (target) {
        case kReplaceAllocator: {
            const auto *config = static_cast<const AllocatorConfig *>(value);
            if (config->malloc) {
                g_config.allocator.malloc = config->malloc;
            }
            if (config->realloc) {
                g_config.allocator.realloc = config->realloc;
            }
            if (config->free) {
                g_config.allocator.free = config->free;
            }
            break;
        }
        case kRestoreAllocator:
            g_config.allocator = kDefaultAllocatorConfig;
            break;
        case kReplaceSyscall:
            return replace_syscall(*reinterpret_cast<const SyscallConfig *>(value));
        case kRestoreSyscall:
            return restore_syscall(reinterpret_cast<const char *>(value));
        default:
            return Status::invalid_argument();
    }
    return Status::ok();
}

} // namespace calicodb
