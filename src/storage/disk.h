#ifndef CCO_STORAGE_ON_DISK_H
#define CCO_STORAGE_ON_DISK_H

#include "calico/status.h"
#include "calico/storage.h"
#include "utils/expect.h"
#include <filesystem>

namespace cco {

class RandomAccessFileReader : public RandomAccessReader {
public:
    RandomAccessFileReader(const std::string &name, int file)
        : m_name {name},
          m_file {file}
    {
        CCO_EXPECT_GE(file, 0);
    }

    ~RandomAccessFileReader() override;
    [[nodiscard]] auto read(Bytes&, Index) -> Status override;

private:
    std::string m_name;
    int m_file {};
};

class RandomAccessFileEditor : public RandomAccessEditor {
public:
    RandomAccessFileEditor(const std::string &name, int file)
        : m_name {name},
          m_file {file}
    {
        CCO_EXPECT_GE(file, 0);
    }

    ~RandomAccessFileEditor() override;
    [[nodiscard]] auto read(Bytes&, Index) -> Status override;
    [[nodiscard]] auto write(BytesView, Index) -> Status override;
    [[nodiscard]] auto sync() -> Status override;

private:
    std::string m_name;
    int m_file {};
};

class AppendFileWriter : public AppendWriter {
public:
    AppendFileWriter(const std::string &name, int file)
        : m_name {name},
          m_file {file}
    {
        CCO_EXPECT_GE(file, 0);
    }

    ~AppendFileWriter() override;
    [[nodiscard]] auto write(BytesView) -> Status override;
    [[nodiscard]] auto sync() -> Status override;

private:
    std::string m_name;
    int m_file {};
};

class DiskStorage: public Storage {
public:
    ~DiskStorage() override = default;
    [[nodiscard]] static auto open(const std::string&, Storage**) -> Status;
    [[nodiscard]] auto create_directory(const std::string&) -> Status override;
    [[nodiscard]] auto remove_directory(const std::string&) -> Status override;
    [[nodiscard]] auto get_file_names(std::vector<std::string>&) const -> Status override;
    [[nodiscard]] auto open_random_reader(const std::string &, RandomAccessReader**) -> Status override;
    [[nodiscard]] auto open_random_editor(const std::string &, RandomAccessEditor**) -> Status override;
    [[nodiscard]] auto open_append_writer(const std::string &, AppendWriter**) -> Status override;
    [[nodiscard]] auto rename_file(const std::string &, const std::string &) -> Status override;
    [[nodiscard]] auto remove_file(const std::string &) -> Status override;
    [[nodiscard]] auto resize_file(const std::string &, Size) -> Status override;
    [[nodiscard]] auto file_exists(const std::string &) const -> Status override;
    [[nodiscard]] auto file_size(const std::string &, Size &) const -> Status override;

private:
    explicit DiskStorage(const std::string &path)
        : m_path {path}
    {}

    std::filesystem::path m_path;
};

} // namespace cco

#endif // CCO_STORAGE_ON_DISK_H
