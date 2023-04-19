// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_ENV_POSIX_H
#define CALICODB_ENV_POSIX_H

#include "calicodb/env.h"
#include "utils.h"

namespace calicodb
{

class PosixFile;
class PosixShm;

class PosixEnv : public Env
{
    friend class PosixFile;

    U16 m_rng[3] = {};
    pid_t m_pid;

public:
    PosixEnv();
    ~PosixEnv() override;
    [[nodiscard]] auto open_file(const std::string &filename, OpenMode mode, File *&out) -> Status override;
    /* TODO: remove */ [[nodiscard]] auto file_exists(const std::string &filename) const -> bool override;

    [[nodiscard]] auto open_shm(const std::string &filename, OpenMode mode, Shm *&out) -> Status override;
    [[nodiscard]] auto close_shm(Shm *&shm) -> Status override;

    [[nodiscard]] auto new_sink(const std::string &filename, Sink *&out) -> Status override;
    [[nodiscard]] auto close_file(File *&file) -> Status override;
    [[nodiscard]] auto remove_file(const std::string &filename) -> Status override;
    [[nodiscard]] auto resize_file(const std::string &filename, std::size_t size) -> Status override;
    [[nodiscard]] auto file_size(const std::string &filename, std::size_t &out) const -> Status override;

    auto srand(unsigned seed) -> void override;
    [[nodiscard]] auto rand() -> unsigned override;
};

// Simple filesystem path management routines
[[nodiscard]] auto split_path(const std::string &filename) -> std::pair<std::string, std::string>;
[[nodiscard]] auto join_paths(const std::string &lhs, const std::string &rhs) -> std::string;
[[nodiscard]] auto cleanup_path(const std::string &filename) -> std::string;

} // namespace calicodb

#endif // CALICODB_ENV_POSIX_H
