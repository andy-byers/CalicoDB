#ifndef CALICO_STORE_DISK_H
#define CALICO_STORE_DISK_H

#include "calico/status.h"
#include "calico/storage.h"
#include "utils/expect.h"
#include <filesystem>

namespace calico {

class RandomFileReader : public RandomReader {
public:
    RandomFileReader(std::string path, int file)
        : m_path {std::move(path)},
          m_file {file}
    {
        CALICO_EXPECT_GE(file, 0);
    }

    ~RandomFileReader() override;
    [[nodiscard]] auto read(Bytes&, Size) -> Status override;

private:
    std::string m_path;
    int m_file {};
};

class RandomFileEditor : public RandomEditor {
public:
    RandomFileEditor(std::string path, int file)
        : m_path {std::move(path)},
          m_file {file}
    {
        CALICO_EXPECT_GE(file, 0);
    }

    ~RandomFileEditor() override;
    [[nodiscard]] auto read(Bytes&, Size) -> Status override;
    [[nodiscard]] auto write(BytesView, Size) -> Status override;
    [[nodiscard]] auto sync() -> Status override;

private:
    std::string m_path;
    int m_file {};
};

class AppendFileWriter : public AppendWriter {
public:
    AppendFileWriter(std::string path, int file)
        : m_path {std::move(path)},
          m_file {file}
    {
        CALICO_EXPECT_GE(file, 0);
    }

    ~AppendFileWriter() override;
    [[nodiscard]] auto write(BytesView) -> Status override;
    [[nodiscard]] auto sync() -> Status override;

private:
    std::string m_path;
    int m_file {};
};

class PosixStorage : public Storage {
public:
    PosixStorage() = default;
    ~PosixStorage() override = default;
    [[nodiscard]] auto create_directory(const std::string&) -> Status override;
    [[nodiscard]] auto remove_directory(const std::string&) -> Status override;
    [[nodiscard]] auto get_children(const std::string&, std::vector<std::string>&) const -> Status override;
    [[nodiscard]] auto open_random_reader(const std::string &, RandomReader **) -> Status override;
    [[nodiscard]] auto open_random_editor(const std::string &, RandomEditor**) -> Status override;
    [[nodiscard]] auto open_append_writer(const std::string &, AppendWriter**) -> Status override;
    [[nodiscard]] auto rename_file(const std::string &, const std::string &) -> Status override;
    [[nodiscard]] auto remove_file(const std::string &) -> Status override;
    [[nodiscard]] auto resize_file(const std::string &, Size) -> Status override;
    [[nodiscard]] auto file_exists(const std::string &) const -> Status override;
    [[nodiscard]] auto file_size(const std::string &, Size &) const -> Status override;
};

} // namespace calico

#endif // CALICO_STORE_DISK_H
