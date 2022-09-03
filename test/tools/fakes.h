#ifndef CALICO_TEST_TOOLS_FAKES_H
#define CALICO_TEST_TOOLS_FAKES_H

#include "calico/calico.h"
#include "calico/storage.h"
#include "pager/pager.h"
#include "random.h"
#include <filesystem>
#include <unordered_set>

namespace calico {

using ReadInterceptor = std::function<Status(const std::string&, Bytes&, Size)>;
using WriteInterceptor = std::function<Status(const std::string&, BytesView, Size)>;
using OpenInterceptor = std::function<Status(const std::string&)>;
using SyncInterceptor = std::function<Status(const std::string&)>;

namespace interceptors {
    auto reset() -> void;
} // namespace interceptors

class RandomHeapReader : public RandomReader {
public:
    RandomHeapReader(std::string name, std::string &file)
        : m_path {std::move(name)},
          m_file {&file}
    {}

    ~RandomHeapReader() override = default;
    [[nodiscard]] auto read(Bytes&, Size) -> Status override;

private:
    std::string m_path;
    std::string *m_file {};
};

class RandomHeapEditor: public RandomEditor {
public:
    RandomHeapEditor(std::string name, std::string &file)
        : m_path {std::move(name)},
          m_file {&file}
    {}

    ~RandomHeapEditor() override = default;
    [[nodiscard]] auto read(Bytes&, Size) -> Status override;
    [[nodiscard]] auto write(BytesView, Size) -> Status override;
    [[nodiscard]] auto sync() -> Status override;

private:
    std::string m_path;
    std::string *m_file {};
};

class AppendHeapWriter: public AppendWriter {
public:
    AppendHeapWriter(std::string name, std::string &file)
        : m_path {std::move(name)},
          m_file {&file}
    {}

    ~AppendHeapWriter() override = default;
    [[nodiscard]] auto write(BytesView) -> Status override;
    [[nodiscard]] auto sync() -> Status override;

private:
    std::string m_path;
    std::string *m_file {};
};

class HeapStorage: public Storage {
public:
    HeapStorage();
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

#endif // CALICO_TEST_TOOLS_FAKES_H
