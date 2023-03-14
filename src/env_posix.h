// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_ENV_POSIX_H
#define CALICODB_ENV_POSIX_H

#include "calicodb/env.h"
#include "calicodb/slice.h"
#include "calicodb/status.h"
#include "utils.h"

namespace calicodb
{

[[nodiscard]] auto split_path(const std::string &filename) -> std::pair<std::string, std::string>;
[[nodiscard]] auto join_paths(const std::string &lhs, const std::string &rhs) -> std::string;

class PosixReader : public Reader
{
public:
    explicit PosixReader(std::string path, int file)
        : m_path {std::move(path)},
          m_file {file}
    {
        CDB_EXPECT_GE(file, 0);
    }

    ~PosixReader() override;
    [[nodiscard]] auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status override;

private:
    std::string m_path;
    int m_file {};
};

class PosixEditor : public Editor
{
public:
    explicit PosixEditor(std::string path, int file)
        : m_path {std::move(path)},
          m_file {file}
    {
        CDB_EXPECT_GE(file, 0);
    }

    ~PosixEditor() override;
    [[nodiscard]] auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status override;
    [[nodiscard]] auto write(std::size_t offset, const Slice &in) -> Status override;
    [[nodiscard]] auto sync() -> Status override;

private:
    std::string m_path;
    int m_file {};
};

class PosixLogger : public Logger
{
public:
    explicit PosixLogger(std::string path, int file)
        : m_path {std::move(path)},
          m_file {file}
    {
        CDB_EXPECT_GE(file, 0);
    }

    ~PosixLogger() override;
    [[nodiscard]] auto write(const Slice &in) -> Status override;
    [[nodiscard]] auto sync() -> Status override;

private:
    std::string m_path;
    int m_file {};
};

class PosixInfoLogger : public InfoLogger
{
public:
    explicit PosixInfoLogger(std::string path, int file)
        : m_buffer(kBufferSize, '\0'),
          m_path {std::move(path)},
          m_file {file}
    {
        CDB_EXPECT_GE(file, 0);
    }

    ~PosixInfoLogger() override;
    auto logv(const char *fmt, ...) -> void override;

private:
    static constexpr std::size_t kBufferSize {512};
    std::string m_buffer;
    std::string m_path;
    int m_file {};
};

class EnvPosix : public Env
{
public:
    EnvPosix() = default;
    ~EnvPosix() override = default;
    [[nodiscard]] auto get_children(const std::string &path, std::vector<std::string> &out) const -> Status override;
    [[nodiscard]] auto new_reader(const std::string &path, Reader *&out) -> Status override;
    [[nodiscard]] auto new_editor(const std::string &path, Editor *&out) -> Status override;
    [[nodiscard]] auto new_logger(const std::string &path, Logger *&out) -> Status override;
    [[nodiscard]] auto new_info_logger(const std::string &path, InfoLogger *&out) -> Status override;
    [[nodiscard]] auto rename_file(const std::string &old_path, const std::string &new_path) -> Status override;
    [[nodiscard]] auto remove_file(const std::string &path) -> Status override;
    [[nodiscard]] auto resize_file(const std::string &path, std::size_t size) -> Status override;
    [[nodiscard]] auto file_exists(const std::string &path) const -> bool override;
    [[nodiscard]] auto file_size(const std::string &path, std::size_t &out) const -> Status override;
};

} // namespace calicodb

#endif // CALICODB_ENV_POSIX_H
