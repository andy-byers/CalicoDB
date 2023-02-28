/*
 * CalicoDB storage environment. The interface is based off of https://github.com/google/leveldb/blob/main/include/leveldb/env.h.
 */
#ifndef CALICO_STORAGE_H
#define CALICO_STORAGE_H

#include "common.h"
#include <string>
#include <vector>

namespace calicodb
{

class Slice;
class Status;

class Reader
{
public:
    virtual ~Reader() = default;
    [[nodiscard]] virtual auto read(Byte *out, Size &size, Size offset) -> Status = 0;
};

class Editor
{
public:
    virtual ~Editor() = default;
    [[nodiscard]] virtual auto read(Byte *out, Size &size, Size offset) -> Status = 0;
    [[nodiscard]] virtual auto write(Slice in, Size offset) -> Status = 0;
    [[nodiscard]] virtual auto sync() -> Status = 0;
};

class Logger
{
public:
    virtual ~Logger() = default;
    [[nodiscard]] virtual auto write(Slice in) -> Status = 0;
    [[nodiscard]] virtual auto sync() -> Status = 0;
};

class InfoLogger
{
public:
    virtual ~InfoLogger() = default;
    virtual auto logv(const char *fmt, ...) -> void = 0;
};

class Storage
{
public:
    static auto default_storage() -> Storage *;

    virtual ~Storage() = default;
    [[nodiscard]] virtual auto create_directory(const std::string &path) -> Status = 0;
    [[nodiscard]] virtual auto remove_directory(const std::string &path) -> Status = 0;
    [[nodiscard]] virtual auto new_reader(const std::string &path, Reader **out) -> Status = 0;
    [[nodiscard]] virtual auto new_editor(const std::string &path, Editor **out) -> Status = 0;
    [[nodiscard]] virtual auto new_logger(const std::string &path, Logger **out) -> Status = 0;
    [[nodiscard]] virtual auto new_info_logger(const std::string &path, InfoLogger **out) -> Status = 0;
    [[nodiscard]] virtual auto get_children(const std::string &path, std::vector<std::string> &out) const -> Status = 0;
    [[nodiscard]] virtual auto rename_file(const std::string &old_path, const std::string &new_path) -> Status = 0;
    [[nodiscard]] virtual auto file_exists(const std::string &path) const -> Status = 0;
    [[nodiscard]] virtual auto resize_file(const std::string &path, Size size) -> Status = 0;
    [[nodiscard]] virtual auto file_size(const std::string &path, Size &out) const -> Status = 0;
    [[nodiscard]] virtual auto remove_file(const std::string &path) -> Status = 0;
};

} // namespace calicodb

#endif // CALICO_STORAGE_H
