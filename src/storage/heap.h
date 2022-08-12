#ifndef CCO_STORAGE_IN_MEMORY_H
#define CCO_STORAGE_IN_MEMORY_H

#include "calico/status.h"
#include "calico/storage.h"
#include "utils/expect.h"
#include <filesystem>
#include <unordered_map>
#include <unordered_set>

namespace cco {

class RandomAccessHeapReader: public RandomAccessReader {
public:
    RandomAccessHeapReader(const std::string &name, std::string &file)
        : m_name {name},
          m_blob {&file}
    {}

    ~RandomAccessHeapReader() override = default;
    [[nodiscard]] auto read(Bytes&, Index) -> Status override;

private:
    std::string m_name;
    std::string *m_blob {};
};

class RandomAccessHeapEditor: public RandomAccessEditor {
public:
    RandomAccessHeapEditor(const std::string &name, std::string &file)
        : m_name {name},
          m_blob {&file}
    {}

    ~RandomAccessHeapEditor() override = default;
    [[nodiscard]] auto read(Bytes&, Index) -> Status override;
    [[nodiscard]] auto write(BytesView, Index) -> Status override;
    [[nodiscard]] auto sync() -> Status override;

private:
    std::string m_name;
    std::string *m_blob {};
};

class AppendHeapWriter: public AppendWriter {
public:
    AppendHeapWriter(const std::string &name, std::string &file)
        : m_name {name},
          m_blob {&file}
    {}

    ~AppendHeapWriter() override = default;
    [[nodiscard]] auto write(BytesView) -> Status override;
    [[nodiscard]] auto sync() -> Status override;

private:
    std::string m_name;
    std::string *m_blob {};
};

class HeapStorage: public Storage {
public:
    HeapStorage() = default;
    ~HeapStorage() override = default;
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
    // TODO: Could use a custom allocator that is better for large blobs.
    std::unordered_map<std::string, std::string> m_files;
    std::unordered_set<std::string> m_directories;
};

} // namespace cco

#endif // CCO_STORAGE_IN_MEMORY_H
