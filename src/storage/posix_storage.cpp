#include "posix_storage.h"
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
            return Status::not_found(strerror(code));
        case EINVAL:
            return Status::invalid_argument(strerror(code));
        case EEXIST:
            return Status::logic_error(strerror(code));
        default:
            return Status::system_error(strerror(code));
    }
}

[[nodiscard]]
static auto errno_to_status() -> Status
{
    const auto code = errno;
    errno = 0;
    return to_status(code);
}

static auto file_open(const std::string &name, int mode, int permissions, int &out) -> Status
{
    if (const auto fd = open(name.c_str(), mode, permissions); fd >= 0) {
        out = fd;
        return Status::ok();
    }
    return errno_to_status();
}

static auto file_close(int fd) -> Status
{
    if (close(fd)) {
        return errno_to_status();
    }
    return Status::ok();
}

static auto file_read(int file, Byte *out, Size &size) -> Status
{
    while (size) {
        const auto n = read(file, out, size);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return errno_to_status();
        }
        size = static_cast<Size>(n);
        break;
    }
    return Status::ok();
}

static auto file_write(int file, Slice in) -> Status
{
    while (!in.is_empty()) {
        if (const auto n = write(file, in.data(), in.size()); n >= 0) {
            in.advance(static_cast<Size>(n));
        } else if (errno != EINTR) {
            return errno_to_status();
        }
    }
    return Status::ok();
}

static auto file_sync(int fd) -> Status
{
    if (fsync(fd)) {
        return errno_to_status();
    }
    return Status::ok();
}

auto file_seek(int fd, long offset, int whence, Size *out) -> Status
{
    if (const auto position = lseek(fd, offset, whence); position != -1) {
        if (out) {
            *out = static_cast<Size>(position);
        }
        return Status::ok();
    }
    return errno_to_status();
}

auto file_remove(const std::string &path) -> Status
{
    if (unlink(path.c_str())) {
        return errno_to_status();
    }
    return Status::ok();
}

auto file_resize(const std::string &path, Size size) -> Status
{
    if (truncate(path.c_str(), static_cast<off_t>(size))) {
        return errno_to_status();
    }
    return Status::ok();
}

auto dir_create(const std::string &path, mode_t permissions) -> Status
{
    if (mkdir(path.c_str(), permissions)) {
        return errno_to_status();
    }
    return Status::ok();
}

auto dir_remove(const std::string &path) -> Status
{
    if (rmdir(path.c_str())) {
        return errno_to_status();
    }
    return Status::ok();
}

PosixReader::~PosixReader()
{
    (void)file_close(m_file);
}

auto PosixReader::read(Byte *out, Size &size, Size offset) -> Status
{
    Calico_Try(file_seek(m_file, static_cast<long>(offset), SEEK_SET, nullptr));
    return file_read(m_file, out, size);
}

PosixEditor::~PosixEditor()
{
    (void)file_close(m_file);
}

auto PosixEditor::read(Byte *out, Size &size, Size offset) -> Status
{
    Calico_Try(file_seek(m_file, static_cast<long>(offset), SEEK_SET, nullptr));
    return file_read(m_file, out, size);
}

auto PosixEditor::write(Slice in, Size offset) -> Status
{
    Calico_Try(file_seek(m_file, static_cast<long>(offset), SEEK_SET, nullptr));
    return file_write(m_file, in);
}

auto PosixEditor::sync() -> Status
{
    return file_sync(m_file);
}

PosixLogger::~PosixLogger()
{
    (void)file_close(m_file);
}

auto PosixLogger::write(Slice in) -> Status
{
    return file_write(m_file, in);
}

auto PosixLogger::sync() -> Status
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
    return Status::ok();
}

auto PosixStorage::remove_file(const std::string &path) -> Status
{
    return file_remove(path);
}

auto PosixStorage::file_exists(const std::string &path) const -> Status
{
    if (struct stat st; stat(path.c_str(), &st)) {
        return Status::not_found("not found");
    }
    return Status::ok();
}

auto PosixStorage::file_size(const std::string &path, Size &out) const -> Status
{
    struct stat st;
    if (stat(path.c_str(), &st)) {
        return errno_to_status();
    }
    out = static_cast<Size>(st.st_size);
    return Status::ok();
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
    return Status::ok();
}

auto PosixStorage::new_reader(const std::string &path, Reader **out) -> Status
{
    int file {};
    Calico_Try(file_open(path, O_RDONLY, FILE_PERMISSIONS, file));
    *out = new(std::nothrow) PosixReader {path, file};
    if (*out == nullptr) {
        return Status::system_error("out of memory");
    }
    return Status::ok();
}

auto PosixStorage::new_editor(const std::string &path, Editor **out) -> Status
{
    int file {};
    Calico_Try(file_open(path, O_CREAT | O_RDWR, FILE_PERMISSIONS, file));
    *out = new(std::nothrow) PosixEditor {path, file};
    if (*out == nullptr) {
        return Status::system_error("out of memory");
    }
    return Status::ok();
}

auto PosixStorage::new_logger(const std::string &path, Logger **out) -> Status
{
    int file {};
    Calico_Try(file_open(path, O_CREAT | O_WRONLY | O_APPEND, FILE_PERMISSIONS, file));
    *out = new(std::nothrow) PosixLogger {path, file};
    if (*out == nullptr) {
        return Status::system_error("out of memory");
    }
    return Status::ok();
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