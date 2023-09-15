// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "config_internal.h"
#include "internal.h"
#include <cstdarg>

namespace calicodb
{

// Per-process configuration options.
Config g_config = {
    {
        CALICODB_DEFAULT_MALLOC,
        CALICODB_DEFAULT_REALLOC,
        CALICODB_DEFAULT_FREE,
    },
};

auto configure(ConfigTarget target, ...) -> Status
{
    std::va_list args;
    va_start(args, target);

    Status s;
    switch (target) {
        case kGetAllocator:
            *va_arg(args, AllocatorConfig *) = g_config.allocator;
            break;
        case kSetAllocator:
            g_config.allocator = va_arg(args, AllocatorConfig);
            CALICODB_EXPECT_NE(g_config.allocator.malloc, nullptr);
            CALICODB_EXPECT_NE(g_config.allocator.realloc, nullptr);
            CALICODB_EXPECT_NE(g_config.allocator.free, nullptr);
            break;
        default:
            s = Status::invalid_argument();
    }
    va_end(args);
    return s;
}

} // namespace calicodb
