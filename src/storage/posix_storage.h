#ifndef CALICO_STORAGE_POSIX_STORAGE_H
#define CALICO_STORAGE_POSIX_STORAGE_H

#include "calico/slice.h"
#include "calico/status.h"
#include "calico/storage.h"
#include "utils/utils.h"

namespace Calico {

class PosixReader : public Reader {
public:
    PosixReader(std::string path, int file)
        : m_path {std::move(path)},
          m_file {file}
    {
        CALICO_EXPECT_GE(file, 0);
    }

    ~PosixReader() override;
    [[nodiscard]] auto read(Byte *out, Size &size, Size offset) -> Status override;

private:
    std::string m_path;
    int m_file {};
};

class PosixEditor : public Editor {
public:
    PosixEditor(std::string path, int file)
        : m_path {std::move(path)},
          m_file {file}
    {
        CALICO_EXPECT_GE(file, 0);
    }

    ~PosixEditor() override;
    [[nodiscard]] auto read(Byte *out, Size &size, Size offset) -> Status override;
    [[nodiscard]] auto write(Slice in, Size size) -> Status override;
    [[nodiscard]] auto sync() -> Status override;

private:
    std::string m_path;
    int m_file {};
};

class PosixLogger : public Logger {
public:
    PosixLogger(std::string path, int file)
        : m_path {std::move(path)},
          m_file {file}
    {
        CALICO_EXPECT_GE(file, 0);
    }

    ~PosixLogger() override;
    [[nodiscard]] auto write(Slice in) -> Status override;
    [[nodiscard]] auto sync() -> Status override;

private:
    std::string m_path;
    int m_file {};
};

class PosixStorage : public Storage {
public:
    PosixStorage() = default;
    ~PosixStorage() override = default;
    [[nodiscard]] auto create_directory(const std::string &path) -> Status override;
    [[nodiscard]] auto remove_directory(const std::string &path) -> Status override;
    [[nodiscard]] auto get_children(const std::string &path, std::vector<std::string> &out) const -> Status override;
    [[nodiscard]] auto new_reader(const std::string &path, Reader **out) -> Status override;
    [[nodiscard]] auto new_editor(const std::string &path, Editor **out) -> Status override;
    [[nodiscard]] auto new_logger(const std::string &path, Logger **out) -> Status override;
    [[nodiscard]] auto rename_file(const std::string &old_path, const std::string &new_path) -> Status override;
    [[nodiscard]] auto remove_file(const std::string &path) -> Status override;
    [[nodiscard]] auto resize_file(const std::string &path, Size size) -> Status override;
    [[nodiscard]] auto file_exists(const std::string &path) const -> Status override;
    [[nodiscard]] auto file_size(const std::string &path, Size &out) const -> Status override;
};

} // namespace Calico

#endif // CALICO_STORAGE_POSIX_STORAGE_H
