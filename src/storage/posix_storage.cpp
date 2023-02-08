#include "posix_storage.h"
#include "utils/expected.hpp"
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>

namespace Calico {

static constexpr int FILE_PERMISSIONS {0644}; // -rw-r--r--

[[nodiscard]]
static auto to_status(int code) -> Status
{
    switch (code) {
        case ENOENT:
            return not_found(strerror(code));
        case EINVAL:
            return invalid_argument(strerror(code));
        case EEXIST:
            return logic_error(strerror(code));
        default:
            return system_error(strerror(code));
    }
}

[[nodiscard]]
static auto fetch_errno() -> int
{
    return std::exchange(errno, 0);
}

[[nodiscard]]
static auto errno_to_status() -> Status
{
    return to_status(fetch_errno());
}

auto file_exists(const std::string &path) -> Status
{
    if (struct stat st; stat(path.c_str(), &st) != 0) {
        return not_found("not found");
    }
    return ok();
}

auto file_open(const std::string &name, int mode, int permissions) -> tl::expected<int, Status>
{
    if (const auto fd = open(name.c_str(), mode, permissions); fd != -1) {
        return fd;
    }
    return tl::make_unexpected(errno_to_status());
}

auto file_close(int fd) -> Status
{
    if (close(fd)) {
        return errno_to_status();
    }
    return ok();
}

auto file_size(const std::string &path) -> tl::expected<Size, Status>
{
    struct stat st;
    if (stat(path.c_str(), &st)) {
        return tl::make_unexpected(errno_to_status());
    }
    return static_cast<Size>(st.st_size);
}

auto file_read(int file, Byte *out, Size size) -> tl::expected<Size, Status>
{
    auto remaining = size;
    for (Size i {}; remaining && i < size; ++i) {
        if (const auto n = read(file, out, remaining); n != -1) {
            remaining -= static_cast<Size>(n);
            out += n;
        } else if (const auto code = fetch_errno(); code != EINTR) {
            return tl::make_unexpected(to_status(code));
        }
    }
    return size - remaining;
}

auto file_write(int file, Slice in) -> tl::expected<Size, Status>
{
    const auto target_size = in.size();
    for (Size i {}; !in.is_empty() && i < target_size; ++i) {
        if (const auto n = write(file, in.data(), in.size()); n != -1) {
            in.advance(static_cast<Size>(n));
        } else if (const auto code = fetch_errno(); code != EINTR) {
            return tl::make_unexpected(to_status(code));
        }
    }
    return target_size - in.size();
}

auto file_sync(int fd) -> Status
{
    if (fsync(fd) == -1) {
        return errno_to_status();
    }
    return ok();
}

auto file_seek(int fd, long offset, int whence) -> tl::expected<Size, Status>
{
    if (const auto position = lseek(fd, offset, whence); position != -1) {
        return static_cast<Size>(position);
    }
    return tl::make_unexpected(errno_to_status());
}

auto file_remove(const std::string &path) -> Status
{
    if (unlink(path.c_str())) {
        return errno_to_status();
    }
    return ok();
}

auto file_resize(const std::string &path, Size size) -> Status
{
    if (truncate(path.c_str(), static_cast<off_t>(size))) {
        return errno_to_status();
    }
    return ok();
}

auto dir_create(const std::string &path, mode_t permissions) -> Status
{
    if (mkdir(path.c_str(), permissions)) {
        if (fetch_errno() == EEXIST) {
            return logic_error("could not create directory: directory {} already exists", path);
        }
        return errno_to_status();
    }
    return ok();
}

auto dir_remove(const std::string &path) -> Status
{
    if (rmdir(path.c_str())) {
        return errno_to_status();
    }
    return ok();
}

static auto read_file_at(int file, Byte *out, Size &requested, Size offset)
{
    auto r = file_seek(file, static_cast<long>(offset), SEEK_SET);
    if (r.has_value()) {
        r = file_read(file, out, requested);
    }

    if (!r.has_value()) {
        return r.error();
    }
    requested = *r;
    return ok();
}

static auto write_file(int file, Slice in)
{
    const auto r = file_write(file, in);

    if (!r.has_value()) {
        return r.error();
    } else if (*r != in.size()) {
        return system_error("could not write to file: incomplete write (wrote {}/{} bytes)", *r, in.size());
    }
    return ok();
}

RandomFileReader::~RandomFileReader()
{
    (void)file_close(m_file);
}

auto RandomFileReader::read(Byte *out, Size &size, Size offset) -> Status
{
    return read_file_at(m_file, out, size, offset);
}

RandomFileEditor::~RandomFileEditor()
{
    (void)file_close(m_file);
}

auto RandomFileEditor::read(Byte *out, Size &size, Size offset) -> Status
{
    return read_file_at(m_file, out, size, offset);
}

auto RandomFileEditor::write(Slice in, Size offset) -> Status
{
    auto r = file_seek(m_file, static_cast<long>(offset), SEEK_SET);
    if (r.has_value()) {
        return write_file(m_file, in);
    }
    return r.error();
}

auto RandomFileEditor::sync() -> Status
{
    return file_sync(m_file);
}

AppendFileWriter::~AppendFileWriter()
{
    (void)file_close(m_file);
}

auto AppendFileWriter::write(Slice in) -> Status
{
    return write_file(m_file, in);
}

auto AppendFileWriter::sync() -> Status
{
    return file_sync(m_file);
}

auto PosixStorage::resize_file(const std::string &path, Size size) -> Status
{
    return file_resize(path, size);
}

auto PosixStorage::rename_file(const std::string &old_path, const std::string &new_path) -> Status
{
    if (rename(old_path.c_str(), new_path.c_str())) {
        return errno_to_status();
    }
    return ok();
}

auto PosixStorage::remove_file(const std::string &path) -> Status
{
    return ::Calico::file_remove(path);
}

auto PosixStorage::file_exists(const std::string &path) const -> Status
{
    return ::Calico::file_exists(path);
}

auto PosixStorage::file_size(const std::string &path, Size &out) const -> Status
{
    if (auto r = ::Calico::file_size(path)) {
        out = *r;
        return ok();
    } else {
        return r.error();
    }
}

auto PosixStorage::get_children(const std::string &path, std::vector<std::string> &out) const -> Status
{
    const auto skip = [](const auto *s) {
        return !std::strcmp(s, ".") || !std::strcmp(s, "..");
    };

    auto *dir = opendir(path.c_str());
    if (dir == nullptr) {
        return errno_to_status();
    }
    struct dirent *ent;
    while ((ent = readdir(dir))) {
        if (skip(ent->d_name)) {
            continue;
        }
        out.emplace_back(ent->d_name);
    }
    closedir(dir);
    return ok();
}

auto PosixStorage::open_random_reader(const std::string &path, RandomReader **out) -> Status
{
    const auto fd = file_open(path, O_RDONLY, FILE_PERMISSIONS);
    if (fd.has_value()) {
        *out = new(std::nothrow) RandomFileReader {path, *fd};
        if (*out == nullptr) {
            return system_error("cannot allocate file: out of memory");
        }
        return ok();
    }
    return fd.error();
}

auto PosixStorage::open_random_editor(const std::string &path, RandomEditor **out) -> Status
{
    const auto fd = file_open(path, O_CREAT | O_RDWR, FILE_PERMISSIONS);
    if (fd.has_value()) {
        *out = new(std::nothrow) RandomFileEditor {path, *fd};
        if (*out == nullptr) {
            return system_error("cannot allocate file: out of memory");
        }
        return ok();
    }
    return fd.error();
}

auto PosixStorage::open_append_writer(const std::string &path, AppendWriter **out) -> Status
{
    const auto fd = file_open(path, O_CREAT | O_WRONLY | O_APPEND, FILE_PERMISSIONS);
    if (fd.has_value()) {
        *out = new(std::nothrow) AppendFileWriter {path, *fd};
        if (*out == nullptr) {
            return system_error("cannot allocate file: out of memory");
        }
        return ok();
    }
    return fd.error();
}

auto PosixStorage::create_directory(const std::string &path) -> Status
{
    return dir_create(path, 0755);
}

auto PosixStorage::remove_directory(const std::string &path) -> Status
{
    return dir_remove(path);
}

} // namespace Calico