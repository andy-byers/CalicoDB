#include "posix_storage.h"
#include "posix_system.h"
#include "utils/expect.h"
#include "utils/system.h"
#include <fcntl.h>

namespace Calico {

namespace fs = std::filesystem;
static constexpr int FILE_PERMISSIONS {0644}; // -rw-r--r--

static auto read_file_at(int file, Byte *out, Size &requested, Size offset)
{
    auto r = Posix::file_seek(file, static_cast<long>(offset), SEEK_SET);
    if (r.has_value())
        r = Posix::file_read(file, out, requested);

    if (!r.has_value())
        return r.error();
    requested = *r;
    return ok();
}

static auto write_file(int file, Slice in)
{
    const auto r = Posix::file_write(file, in);

    if (!r.has_value()) {
        return r.error();
    } else if (*r != in.size()) {
        return system_error("could not write to file: incomplete write (wrote {}/{} span)", *r, in.size());
    }
    return ok();
}

RandomFileReader::~RandomFileReader()
{
    [[maybe_unused]] const auto s = Posix::file_close(m_file);
}

auto RandomFileReader::read(Byte *out, Size &size, Size offset) -> Status
{
    return read_file_at(m_file, out, size, offset);
}

RandomFileEditor::~RandomFileEditor()
{
    [[maybe_unused]] const auto s = Posix::file_close(m_file);
}

auto RandomFileEditor::read(Byte *out, Size &size, Size offset) -> Status
{
    return read_file_at(m_file, out, size, offset);
}

auto RandomFileEditor::write(Slice in, Size offset) -> Status
{
    auto r = Posix::file_seek(m_file, static_cast<long>(offset), SEEK_SET);
    if (r.has_value())
        return write_file(m_file, in);
    return r.error();
}

auto RandomFileEditor::sync() -> Status
{
    return Posix::file_sync(m_file);
}

AppendFileWriter::~AppendFileWriter()
{
    [[maybe_unused]] const auto s = Posix::file_close(m_file);
}

auto AppendFileWriter::write(Slice in) -> Status
{
    return write_file(m_file, in);
}

auto AppendFileWriter::sync() -> Status
{
    return Posix::file_sync(m_file);
}

auto PosixStorage::resize_file(const std::string &path, Size size) -> Status
{
    return Posix::file_resize(path, size);
}

auto PosixStorage::rename_file(const std::string &old_path, const std::string &new_path) -> Status
{
    std::error_code code;
    fs::rename(old_path, new_path, code);
    if (code) return system_error(code.message());
    return ok();
}

auto PosixStorage::remove_file(const std::string &path) -> Status
{
    return Posix::file_remove(path);
}

auto PosixStorage::file_exists(const std::string &path) const -> Status
{
    return Posix::file_exists(path);
}

auto PosixStorage::file_size(const std::string &path, Size &out) const -> Status
{
    auto r = Posix::file_size(path);
    if (r.has_value()) {
        out = *r;
        return ok();
    }
    return r.error();
}

auto PosixStorage::get_children(const std::string &path, std::vector<std::string> &out) const -> Status
{
    std::error_code error;
    std::filesystem::directory_iterator itr {path, error};
    if (error) return system_error(error.message());

    for (auto const &entry: itr)
        out.emplace_back(entry.path());
    return ok();
}

auto PosixStorage::open_random_reader(const std::string &path, RandomReader **out) -> Status
{
    const auto fd = Posix::file_open(path, O_RDONLY, FILE_PERMISSIONS);
    if (fd.has_value()) {
        *out = new(std::nothrow) RandomFileReader {path, *fd};
        if (*out == nullptr)
            return system_error("cannot allocate file: out of memory");
        return ok();
    }
    return fd.error();
}

auto PosixStorage::open_random_editor(const std::string &path, RandomEditor **out) -> Status
{
    const auto fd = Posix::file_open(path, O_CREAT | O_RDWR, FILE_PERMISSIONS);
    if (fd.has_value()) {
        *out = new(std::nothrow) RandomFileEditor {path, *fd};
        if (*out == nullptr)
            return system_error("cannot allocate file: out of memory");
        return ok();
    }
    return fd.error();
}

auto PosixStorage::open_append_writer(const std::string &path, AppendWriter **out) -> Status
{
    const auto fd = Posix::file_open(path, O_CREAT | O_WRONLY | O_APPEND, FILE_PERMISSIONS);
    if (fd.has_value()) {
        *out = new(std::nothrow) AppendFileWriter {path, *fd};
        if (*out == nullptr)
            return system_error("cannot allocate file: out of memory");
        return ok();
    }
    return fd.error();
}

auto PosixStorage::create_directory(const std::string &path) -> Status
{
    return Posix::dir_create(path, 0755);
}

auto PosixStorage::remove_directory(const std::string &path) -> Status
{
    return Posix::dir_remove(path);
}

} // namespace Calico