// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source Status::Code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_TEMP_H
#define CALICODB_TEMP_H

#include <cstdlib>

namespace calicodb
{

class Env;
class Wal;
struct Stats;
struct WalOptionsExtra;

[[nodiscard]] auto new_temp_env(size_t sector_size) -> Env *;
[[nodiscard]] auto new_temp_wal(const WalOptionsExtra &options) -> Wal *;

} // namespace calicodb

#endif // CALICODB_TEMP_H
