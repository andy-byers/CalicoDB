#ifndef CALICO_STORE_HEAP_H
#define CALICO_STORE_HEAP_H

#include "calico/status.h"
#include "calico/store.h"
#include "utils/expect.h"
#include <filesystem>
#include <unordered_map>
#include <unordered_set>

namespace calico {

class RandomHeapReader : public RandomReader {
public:
    RandomHeapReader(std::string name, std::string &file)
        : m_name {std::move(name)},
          m_file {&file}
    {}

    ~RandomHeapReader() override = default;
    [[nodiscard]] auto read(Bytes&, Size) -> Status override;

private:
    std::string m_name;
    std::string *m_file {};
};

class RandomHeapEditor: public RandomEditor {
public:
    RandomHeapEditor(std::string name, std::string &file)
        : m_name {std::move(name)},
          m_file {&file}
    {}

    ~RandomHeapEditor() override = default;
    [[nodiscard]] auto read(Bytes&, Size) -> Status override;
    [[nodiscard]] auto write(BytesView, Size) -> Status override;
    [[nodiscard]] auto sync() -> Status override;

private:
    std::string m_name;
    std::string *m_file {};
};

class AppendHeapWriter: public AppendWriter {
public:
    AppendHeapWriter(std::string name, std::string &file)
        : m_name {std::move(name)},
          m_file {&file}
    {}

    ~AppendHeapWriter() override = default;
    [[nodiscard]] auto write(BytesView) -> Status override;
    [[nodiscard]] auto sync() -> Status override;

private:
    std::string m_name;
    std::string *m_file {};
};

class HeapStorage: public Storage {
public:
    HeapStorage() = default;
    ~HeapStorage() override = default;
    [[nodiscard]] auto create_directory(const std::string&) -> Status override;
    [[nodiscard]] auto remove_directory(const std::string&) -> Status override;
    [[nodiscard]] auto get_children(const std::string&, std::vector<std::string>&) const -> Status override;
    [[nodiscard]] auto open_random_reader(const std::string &, RandomReader**) -> Status override;
    [[nodiscard]] auto open_random_editor(const std::string &, RandomEditor**) -> Status override;
    [[nodiscard]] auto open_append_writer(const std::string &, AppendWriter**) -> Status override;
    [[nodiscard]] auto rename_file(const std::string &, const std::string &) -> Status override;
    [[nodiscard]] auto remove_file(const std::string &) -> Status override;
    [[nodiscard]] auto resize_file(const std::string &, Size) -> Status override;
    [[nodiscard]] auto file_exists(const std::string &) const -> Status override;
    [[nodiscard]] auto file_size(const std::string &, Size &) const -> Status override;
    auto clone() const -> Storage*;

private:
    mutable std::mutex m_mutex;
    // TODO: Could use a custom allocator that is better for large contiguous chunks.
    std::unordered_map<std::string, std::string> m_files;
    std::unordered_set<std::string> m_directories;
};

} // namespace calico

#endif // CALICO_STORE_HEAP_H
