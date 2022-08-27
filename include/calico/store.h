#ifndef CALICO_STORE_H
#define CALICO_STORE_H

#include "calico/status.h"
#include <string>

namespace calico {

/**
 * An interface for reading bytes at a specified location.
 */
class RandomReader {
public:
    virtual ~RandomReader() = default;
    [[nodiscard]] virtual auto read(Bytes&, Size) -> Status = 0;
};

/**
 * An interface for reading and writing bytes at specified locations.
 */
class RandomEditor {
public:
    virtual ~RandomEditor() = default;
    [[nodiscard]] virtual auto read(Bytes&, Size) -> Status = 0;
    [[nodiscard]] virtual auto write(BytesView, Size) -> Status = 0;
    [[nodiscard]] virtual auto sync() -> Status = 0;
};

/**
 * An interface for appending bytes.
 */
class AppendWriter {
public:
    virtual ~AppendWriter() = default;
    [[nodiscard]] virtual auto write(BytesView) -> Status = 0;
    [[nodiscard]] virtual auto sync() -> Status = 0;
};

/**
 * An abstraction that provides a storage environment for a Calico DB database to operate within.
 */
class Storage {
public:
    virtual ~Storage() = default;
    [[nodiscard]] virtual auto create_directory(const std::string &) -> Status = 0;
    [[nodiscard]] virtual auto remove_directory(const std::string &) -> Status = 0;
    [[nodiscard]] virtual auto open_random_reader(const std::string &, RandomReader**) -> Status = 0;
    [[nodiscard]] virtual auto open_random_editor(const std::string &, RandomEditor**) -> Status = 0;
    [[nodiscard]] virtual auto open_append_writer(const std::string &, AppendWriter**) -> Status = 0;
    [[nodiscard]] virtual auto get_children(const std::string&, std::vector<std::string>&) const -> Status = 0;
    [[nodiscard]] virtual auto rename_file(const std::string &, const std::string &) -> Status = 0;
    [[nodiscard]] virtual auto file_exists(const std::string &) const -> Status = 0;
    [[nodiscard]] virtual auto resize_file(const std::string &, Size) -> Status = 0;
    [[nodiscard]] virtual auto file_size(const std::string &, Size &) const -> Status = 0;

    /**
     * Remove a blob from the storage object.
     *
     * Once this method is called on a name N, calling any method (besides the destructor) on a live child with name N will result in undefined behavior.
     *
     * @param name Name of the child to remove.
     * @return A status object indicating success or failure.
     */
    [[nodiscard]] virtual auto remove_file(const std::string &name) -> Status = 0;
};

template<class Reader>
[[nodiscard]]
auto read_exact(Reader &reader, Bytes out, Size offset) -> Status
{
    static constexpr auto FMT = "could not read exact: read {}/{} bytes";
    const auto requested = out.size();
    auto s = reader.read(out, offset);

    if (s.is_ok() && out.size() != requested)
        return Status::system_error(fmt::format(FMT, out.size(), requested));

    return s;
}

} // namespace calico

#endif // CALICO_STORE_H
