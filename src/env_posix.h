// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_ENV_POSIX_H
#define CALICODB_ENV_POSIX_H

#include "calicodb/env.h"
#include "utils.h"

namespace calicodb
{

class PosixEnv : public Env
{
    uint16_t m_rng[3] = {};

public:
    explicit PosixEnv();
    ~PosixEnv() override = default;

    auto new_logger(const char *filename, Logger *&out) -> Status override;
    auto new_file(const char *filename, OpenMode mode, File *&out) -> Status override;
    [[nodiscard]] auto file_exists(const char *filename) const -> bool override;
    auto remove_file(const char *filename) -> Status override;
    [[nodiscard]] auto file_size(const char *filename, size_t &out) const -> Status override;

    auto srand(unsigned seed) -> void override;
    [[nodiscard]] auto rand() -> unsigned override;

    auto sleep(unsigned micros) -> void override;
};

} // namespace calicodb

#endif // CALICODB_ENV_POSIX_H
