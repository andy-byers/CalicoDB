// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "config_internal.h"
#include "internal.h"
#include <cstdarg>

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

auto configure(ConfigTarget target, void *value) -> Status
{
    Status s;
    switch (target) {
        case kGetAllocator:
            *static_cast<AllocatorConfig *>(value) = g_config.allocator;
            break;
        case kSetAllocator:
            g_config.allocator = value ? *static_cast<const AllocatorConfig *>(value)
                                       : kDefaultAllocatorConfig;
            CALICODB_EXPECT_NE(g_config.allocator.malloc, nullptr);
            CALICODB_EXPECT_NE(g_config.allocator.realloc, nullptr);
            CALICODB_EXPECT_NE(g_config.allocator.free, nullptr);
            break;
        default:
            s = Status::invalid_argument();
    }
    return s;
}

} // namespace calicodb
