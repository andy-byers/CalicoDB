// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_CONFIG_INTERNAL_H
#define CALICODB_CONFIG_INTERNAL_H

#include "calicodb/config.h"
#include "calicodb/status.h"

namespace calicodb
{

extern struct Config {
    AllocatorConfig allocator;
} g_config;

} // namespace calicodb

#endif // CALICODB_CONFIG_INTERNAL_H
