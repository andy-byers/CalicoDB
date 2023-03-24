// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_ENV_POSIX_H
#define CALICODB_ENV_POSIX_H

#include "calicodb/env.h"
#include "calicodb/slice.h"
#include "calicodb/status.h"

namespace calicodb
{

[[nodiscard]] auto split_path(const std::string &filename) -> std::pair<std::string, std::string>;
[[nodiscard]] auto join_paths(const std::string &lhs, const std::string &rhs) -> std::string;

class PosixReader : public Reader
{
public:
    explicit PosixReader(std::string filename, int file);
    ~PosixReader() override;
    [[nodiscard]] auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status override;

private:
    std::string m_filename;
    int m_file {};
};

class PosixEditor : public Editor
{
public:
    explicit PosixEditor(std::string filename, int file);
    ~PosixEditor() override;
    [[nodiscard]] auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status override;
    [[nodiscard]] auto write(std::size_t offset, const Slice &in) -> Status override;
    [[nodiscard]] auto sync() -> Status override;

private:
    std::string m_filename;
    int m_file {};
};

class PosixLogger : public Logger
{
public:
    explicit PosixLogger(std::string filename, int file);
    ~PosixLogger() override;
    [[nodiscard]] auto write(const Slice &in) -> Status override;
    [[nodiscard]] auto sync() -> Status override;

private:
    std::string m_filename;
    int m_file {};
};

class PosixInfoLogger : public InfoLogger
{
public:
    explicit PosixInfoLogger(std::string filename, int file);
    ~PosixInfoLogger() override;
    auto logv(const char *fmt, ...) -> void override;

private:
    static constexpr std::size_t kBufferSize = 512;
    std::string m_buffer;
    std::string m_filename;
    int m_file {};
};

class EnvPosix : public Env
{
public:
    EnvPosix() = default;
    ~EnvPosix() override = default;
    [[nodiscard]] auto get_children(const std::string &dirname, std::vector<std::string> &out) const -> Status override;
    [[nodiscard]] auto new_reader(const std::string &filename, Reader *&out) -> Status override;
    [[nodiscard]] auto new_editor(const std::string &filename, Editor *&out) -> Status override;
    [[nodiscard]] auto new_logger(const std::string &filename, Logger *&out) -> Status override;
    [[nodiscard]] auto new_info_logger(const std::string &filename, InfoLogger *&out) -> Status override;
    [[nodiscard]] auto rename_file(const std::string &old_filename, const std::string &new_filename) -> Status override;
    [[nodiscard]] auto remove_file(const std::string &filename) -> Status override;
    [[nodiscard]] auto resize_file(const std::string &filename, std::size_t size) -> Status override;
    [[nodiscard]] auto file_exists(const std::string &filename) const -> bool override;
    [[nodiscard]] auto file_size(const std::string &filename, std::size_t &out) const -> Status override;
    [[nodiscard]] auto sync_directory(const std::string &dirname) -> Status override;
};

} // namespace calicodb

#endif // CALICODB_ENV_POSIX_H
