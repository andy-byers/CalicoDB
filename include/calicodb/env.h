// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for contributor names.

#ifndef CALICODB_ENV_H
#define CALICODB_ENV_H

#include <string>
#include <vector>

namespace calicodb
{

class Slice;
class Status;

// Representation of a read-only random-access file.
class Reader
{
public:
    virtual ~Reader() = default;
    [[nodiscard]] virtual auto read(char *out, std::size_t *size, std::size_t offset) -> Status = 0;
};

// Representation of a random-access file.
class Editor
{
public:
    virtual ~Editor() = default;
    [[nodiscard]] virtual auto read(char *out, std::size_t *size, std::size_t offset) -> Status = 0;
    [[nodiscard]] virtual auto write(Slice in, std::size_t offset) -> Status = 0;
    [[nodiscard]] virtual auto sync() -> Status = 0;
};

// Representation of a write-only append-only file.
class Logger
{
public:
    virtual ~Logger() = default;
    [[nodiscard]] virtual auto write(Slice in) -> Status = 0;
    [[nodiscard]] virtual auto sync() -> Status = 0;
};

// Construct for logging information about the program.
class InfoLogger
{
public:
    virtual ~InfoLogger() = default;
    virtual auto logv(const char *fmt, ...) -> void = 0;
};

// CalicoDB storage environment.
//
// Performs platform-specific filesystem manipulations and manages allocation of I/O
// helper objects (Reader, Editor, etc.).
class Env
{
public:
    static auto default_env() -> Env *;

    virtual ~Env() = default;
    [[nodiscard]] virtual auto new_reader(const std::string &path, Reader **out) -> Status = 0;
    [[nodiscard]] virtual auto new_editor(const std::string &path, Editor **out) -> Status = 0;
    [[nodiscard]] virtual auto new_logger(const std::string &path, Logger **out) -> Status = 0;
    [[nodiscard]] virtual auto new_info_logger(const std::string &path, InfoLogger **out) -> Status = 0;
    [[nodiscard]] virtual auto get_children(const std::string &path, std::vector<std::string> *out) const -> Status = 0;
    [[nodiscard]] virtual auto rename_file(const std::string &old_path, const std::string &new_path) -> Status = 0;
    [[nodiscard]] virtual auto file_exists(const std::string &path) const -> Status = 0;
    [[nodiscard]] virtual auto resize_file(const std::string &path, std::size_t size) -> Status = 0;
    [[nodiscard]] virtual auto file_size(const std::string &path, std::size_t *out) const -> Status = 0;
    [[nodiscard]] virtual auto remove_file(const std::string &path) -> Status = 0;
};

} // namespace calicodb

#endif // CALICODB_ENV_H
