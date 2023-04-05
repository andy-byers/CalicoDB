// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_ENV_H
#define CALICODB_ENV_H

#include "status.h"
#include <vector>

namespace calicodb
{

class File;
class LogFile;

// CalicoDB storage environment.
//
// Handles platform-specific filesystem manipulations and manages allocation of I/O
// helper objects (Reader, Editor, etc.).
class Env
{
public:
    static auto default_env() -> Env *;

    virtual ~Env();
    [[nodiscard]] virtual auto new_file(const std::string &path, File *&out) -> Status = 0;
    [[nodiscard]] virtual auto new_log_file(const std::string &path, LogFile *&out) -> Status = 0;
    [[nodiscard]] virtual auto get_children(const std::string &path, std::vector<std::string> &out) const -> Status = 0;
    [[nodiscard]] virtual auto rename_file(const std::string &old_path, const std::string &new_path) -> Status = 0;
    [[nodiscard]] virtual auto file_exists(const std::string &path) const -> bool = 0;
    [[nodiscard]] virtual auto resize_file(const std::string &path, std::size_t size) -> Status = 0;
    [[nodiscard]] virtual auto file_size(const std::string &path, std::size_t &out) const -> Status = 0;
    [[nodiscard]] virtual auto remove_file(const std::string &path) -> Status = 0;
};

class File
{
public:
    virtual ~File();

    // Attempt to read "size" bytes from the file at the given offset.
    //
    // Reads into "scratch", which must point to at least "size" bytes of available
    // memory. On success, sets "*out" to point to the data that was read, which may
    // be less than what was requested, but only if "out" is not nullptr.
    [[nodiscard]] virtual auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status = 0;

    // Read exactly "size" bytes from the file at the given offset.
    //
    // Return a "not found" status if there is not enough data remaining in the file.
    [[nodiscard]] virtual auto read_exact(std::size_t offset, std::size_t size, char *scratch) -> Status;

    // Write "in" to the file at the given offset.
    [[nodiscard]] virtual auto write(std::size_t offset, const Slice &in) -> Status = 0;

    // Synchronize with the underlying filesystem.
    [[nodiscard]] virtual auto sync() -> Status = 0;
};

// Construct for logging information about the program.
class LogFile
{
public:
    virtual ~LogFile();

    // Write a message to the info log using format string "fmt", which must be a
    // string literal.
    virtual auto logv(const char *fmt, ...) -> void = 0;
};

class EnvWrapper : public Env
{
public:
    explicit EnvWrapper(Env &env);
    [[nodiscard]] auto target() -> Env *;

    ~EnvWrapper() override;
    [[nodiscard]] auto new_file(const std::string &path, File *&out) -> Status override;
    [[nodiscard]] auto new_log_file(const std::string &path, LogFile *&out) -> Status override;
    [[nodiscard]] auto get_children(const std::string &dirname, std::vector<std::string> &out) const -> Status override;
    [[nodiscard]] auto rename_file(const std::string &old_filename, const std::string &new_filename) -> Status override;
    [[nodiscard]] auto file_exists(const std::string &filename) const -> bool override;
    [[nodiscard]] auto resize_file(const std::string &filename, std::size_t size) -> Status override;
    [[nodiscard]] auto file_size(const std::string &filename, std::size_t &out) const -> Status override;
    [[nodiscard]] auto remove_file(const std::string &filename) -> Status override;

private:
    Env *m_target;
};

} // namespace calicodb

#endif // CALICODB_ENV_H
