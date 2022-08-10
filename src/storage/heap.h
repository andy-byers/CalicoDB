#ifndef CCO_STORAGE_IN_MEMORY_H
#define CCO_STORAGE_IN_MEMORY_H

#include "calico/status.h"
#include "calico/storage.h"
#include "utils/expect.h"
#include <filesystem>

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
    [[nodiscard]] auto get_blob_names(std::vector<std::string>&) const -> Status override;
    [[nodiscard]] auto open_random_access_reader(const std::string &, RandomAccessReader**) -> Status override;
    [[nodiscard]] auto open_random_access_editor(const std::string &, RandomAccessEditor**) -> Status override;
    [[nodiscard]] auto open_append_writer(const std::string &, AppendWriter**) -> Status override;
    [[nodiscard]] auto rename_blob(const std::string &, const std::string &) -> Status override;
    [[nodiscard]] auto remove_blob(const std::string &) -> Status override;
    [[nodiscard]] auto resize_blob(const std::string &, Size) -> Status override;
    [[nodiscard]] auto blob_exists(const std::string &) const -> Status override;
    [[nodiscard]] auto blob_size(const std::string &, Size &) const -> Status override;

private:
    // TODO: Could use a custom allocator that is better for large blobs.
    std::unordered_map<std::string, std::string> m_blobs;
};

} // namespace cco

#endif // CCO_STORAGE_IN_MEMORY_H
