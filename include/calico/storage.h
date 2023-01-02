/*
 * Calico DB storage environment. The interface is based off of https://github.com/google/leveldb/blob/main/include/leveldb/env.h.
 */
#ifndef CALICO_STORAGE_H
#define CALICO_STORAGE_H

#include "calico/status.h"
#include <string>
#include <vector>

namespace Calico {

class RandomReader {
public:
    virtual ~RandomReader() = default;
    [[nodiscard]] virtual auto read(Bytes &out, Size offset) -> Status = 0;
};

class RandomEditor {
public:
    virtual ~RandomEditor() = default;
    [[nodiscard]] virtual auto read(Bytes &out, Size offset) -> Status = 0;
    [[nodiscard]] virtual auto write(BytesView in, Size offset) -> Status = 0;
    [[nodiscard]] virtual auto sync() -> Status = 0;
};

class AppendWriter {
public:
    virtual ~AppendWriter() = default;
    [[nodiscard]] virtual auto write(BytesView in) -> Status = 0;
    [[nodiscard]] virtual auto sync() -> Status = 0;
};

class Storage {
public:
    virtual ~Storage() = default;
    [[nodiscard]] virtual auto create_directory(const std::string &path) -> Status = 0;
    [[nodiscard]] virtual auto remove_directory(const std::string &path) -> Status = 0;
    [[nodiscard]] virtual auto open_random_reader(const std::string &path, RandomReader **out) -> Status = 0;
    [[nodiscard]] virtual auto open_random_editor(const std::string &path, RandomEditor **out) -> Status = 0;
    [[nodiscard]] virtual auto open_append_writer(const std::string &path, AppendWriter **out) -> Status = 0;
    [[nodiscard]] virtual auto get_children(const std::string &path, std::vector<std::string> &out) const -> Status = 0;
    [[nodiscard]] virtual auto rename_file(const std::string &old_path, const std::string &new_path) -> Status = 0;
    [[nodiscard]] virtual auto file_exists(const std::string &path) const -> Status = 0;
    [[nodiscard]] virtual auto resize_file(const std::string &path, Size size) -> Status = 0;
    [[nodiscard]] virtual auto file_size(const std::string &path, Size &out) const -> Status = 0;
    [[nodiscard]] virtual auto remove_file(const std::string &path) -> Status = 0;
};

} // namespace Calico

#endif // CALICO_STORAGE_H
