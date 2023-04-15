// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_ENV_POSIX_H
#define CALICODB_ENV_POSIX_H

#include "calicodb/env.h"
#include "utils.h"

namespace calicodb
{

[[nodiscard]] auto split_path(const std::string &filename) -> std::pair<std::string, std::string>;
[[nodiscard]] auto join_paths(const std::string &lhs, const std::string &rhs) -> std::string;
[[nodiscard]] auto cleanup_path(const std::string &filename) -> std::string;

class PosixFile : public File
{
public:
    explicit PosixFile(std::string filename, int file);
    ~PosixFile() override;
    [[nodiscard]] auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status override;
    [[nodiscard]] auto write(std::size_t offset, const Slice &in) -> Status override;
    [[nodiscard]] auto sync() -> Status override;

private:
    friend class EnvPosix;
    std::string m_filename;
    int m_file = -1;
    int m_lock = 0;
};

class PosixLogFile : public LogFile
{
public:
    explicit PosixLogFile(std::string filename, int file);
    ~PosixLogFile() override;
    auto write(const Slice &in) -> void override;

private:
    static constexpr std::size_t kBufferSize = 512;
    std::string m_buffer;
    std::string m_filename;
    int m_file = -1;
};

class EnvPosix : public Env
{
    U16 m_rng[3] = {};
    pid_t m_pid;

public:
    EnvPosix();
    ~EnvPosix() override = default;
    [[nodiscard]] auto new_file(const std::string &filename, File *&out) -> Status override;
    [[nodiscard]] auto new_log_file(const std::string &filename, LogFile *&out) -> Status override;
    [[nodiscard]] auto remove_file(const std::string &filename) -> Status override;
    [[nodiscard]] auto resize_file(const std::string &filename, std::size_t size) -> Status override;
    [[nodiscard]] auto file_exists(const std::string &filename) const -> bool override;
    [[nodiscard]] auto file_size(const std::string &filename, std::size_t &out) const -> Status override;

    auto srand(unsigned seed) -> void override;
    [[nodiscard]] auto rand() -> unsigned override;

    [[nodiscard]] auto lock(File &file, LockMode mode) -> Status override;
    [[nodiscard]] auto unlock(File &file) -> Status override;
};

} // namespace calicodb

#endif // CALICODB_ENV_POSIX_H
