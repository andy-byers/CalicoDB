#ifndef CCO_STORAGE_H
#define CCO_STORAGE_H

#include "calico/status.h"
#include "utils/result.h"
#include <fcntl.h>
#include <memory>
#include <string>
#include <vector>

namespace cco {

class RandomAccessReader {
public:
    virtual ~RandomAccessReader() = default;
    virtual auto read(Bytes&, Index) -> Status = 0;
};

class RandomAccessEditor {
public:
    virtual ~RandomAccessEditor() = default;
    virtual auto read(Bytes&, Index) -> Status = 0;
    virtual auto write(BytesView, Index) -> Status = 0;
    virtual auto sync() -> Status = 0;
};

class AppendWriter {
public:
    virtual ~AppendWriter() = default;
    virtual auto write(BytesView) -> Status = 0;
    virtual auto sync() -> Status = 0;
};

class Storage {
public:
    virtual ~Storage() = default;
    virtual auto create_directory(const std::string &) -> Status = 0;
    virtual auto remove_directory(const std::string &) -> Status = 0;
    virtual auto open_random_reader(const std::string &, RandomAccessReader**) -> Status = 0;
    virtual auto open_random_editor(const std::string &, RandomAccessEditor**) -> Status = 0;
    virtual auto open_append_writer(const std::string &, AppendWriter**) -> Status = 0;
    virtual auto get_file_names(std::vector<std::string>&) const -> Status = 0;
    virtual auto rename_file(const std::string &, const std::string &) -> Status = 0;
    virtual auto file_exists(const std::string &) const -> Status = 0;
    virtual auto resize_file(const std::string &, Size) -> Status = 0;
    virtual auto file_size(const std::string &, Size &) const -> Status = 0;

    /**
     * Remove a blob from the storage object.
     *
     * Once this method is called on a name N, calling any method (besides the destructor) on a live child with name N will result in undefined behavior.
     *
     * @param name Name of the child to remove.
     * @return A status object indicating success or failure.
     */
    virtual auto remove_file(const std::string &name) -> Status = 0;
};

template<class Reader>
auto read_exact(Reader &reader, Bytes out, Index offset) -> Status
{
    static constexpr auto ERROR_FMT = "could not read exact: read {}/{} bytes";
    const auto requested = out.size();
    auto s = reader.file_read(out, offset);
    if (s.is_ok() && out.size() != requested) {
        const auto message = fmt::format(ERROR_FMT, out.size(), requested);
        s = Status::system_error(message);
    }
    return s;
}

} // namespace cco

#endif // CCO_STORAGE_H
