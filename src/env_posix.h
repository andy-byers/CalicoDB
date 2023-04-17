// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_ENV_POSIX_H
#define CALICODB_ENV_POSIX_H

#include "calicodb/env.h"
#include "utils.h"

namespace calicodb
{

// Representation of an open file descriptor
class PosixFile;

// Simple filesystem path management routines
[[nodiscard]] auto split_path(const std::string &filename) -> std::pair<std::string, std::string>;
[[nodiscard]] auto join_paths(const std::string &lhs, const std::string &rhs) -> std::string;
[[nodiscard]] auto cleanup_path(const std::string &filename) -> std::string;

class PosixEnv : public Env
{
    friend class PosixFile;

    static auto ref_inode(PosixFile &file) -> int;
    static auto unref_inode(PosixFile &file) -> void;
    static auto close_pending_files(PosixFile &file) -> void;
    static auto set_pending_file(PosixFile &file) -> void;

    U16 m_rng[3] = {};
    pid_t m_pid;

public:
    PosixEnv();
    ~PosixEnv() override;
    [[nodiscard]] auto new_file(const std::string &filename, OpenMode mode, File *&out) -> Status override;
    [[nodiscard]] auto new_log_file(const std::string &filename, LogFile *&out) -> Status override;
    [[nodiscard]] auto remove_file(const std::string &filename) -> Status override;
    [[nodiscard]] auto resize_file(const std::string &filename, std::size_t size) -> Status override;
    [[nodiscard]] auto file_exists(const std::string &filename) const -> bool override;
    [[nodiscard]] auto file_size(const std::string &filename, std::size_t &out) const -> Status override;

    auto srand(unsigned seed) -> void override;
    [[nodiscard]] auto rand() -> unsigned override;

    [[nodiscard]] auto set_lock(File &file, LockMode mode) -> Status override;
    [[nodiscard]] auto get_lock(const File &file) const -> LockMode override;
    [[nodiscard]] auto unlock(File &file, LockMode mode) -> Status override;
};

} // namespace calicodb

#endif // CALICODB_ENV_POSIX_H
