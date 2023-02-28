#include "calicodb/env.h"
#include "env_posix.h"

namespace calicodb
{

auto Env::default_env() -> Env *
{
    return new PosixEnv;
}

} // namespace calicodb