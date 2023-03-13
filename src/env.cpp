// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for contributor names.

#include "calicodb/env.h"
#include "env_posix.h"

namespace calicodb
{

auto Env::default_env() -> Env *
{
    return new EnvPosix;
}

} // namespace calicodb