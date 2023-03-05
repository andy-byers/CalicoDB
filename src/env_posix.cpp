#include "env_posix.h"
#include <cstdarg>
#include <dirent.h>
#include <fcntl.h>
#include <libgen.h>
#include <sys/stat.h>
#include <unistd.h>

namespace calicodb
{

static constexpr int kFilePermissions {0644}; // -rw-r--r--

[[nodiscard]] static auto to_status(int code) -> Status
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

[[nodiscard]] static auto errno_to_status() -> Status
{
    const auto code = errno;
    errno = 0;
    return to_status(code);
}

static auto file_open(const std::string &name, int mode, int permissions, int *out) -> Status
{
    if (const auto fd = open(name.c_str(), mode, permissions); fd >= 0) {
        *out = fd;
        return Status::ok();
    }
    *out = -1;
    return errno_to_status();
}

static auto file_close(int fd) -> Status
{
    if (close(fd)) {
        return errno_to_status();
    }
    return Status::ok();
}

static auto file_read(int file, char *out, std::size_t *size) -> Status
{
    for (; ; ) {
        const auto n = read(file, out, *size);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return errno_to_status();
        }
        *size = static_cast<std::size_t>(n);
        break;
    }
    return Status::ok();
}

static auto file_write(int file, Slice in) -> Status
{
    while (!in.is_empty()) {
        if (const auto n = write(file, in.data(), in.size()); n >= 0) {
            in.advance(static_cast<std::size_t>(n));
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

auto file_seek(int fd, long offset, int whence, std::size_t *out) -> Status
{
    if (const auto position = lseek(fd, offset, whence); position != -1) {
        if (out) {
            *out = static_cast<std::size_t>(position);
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

auto file_resize(const std::string &path, std::size_t size) -> Status
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

auto PosixReader::read(char *out, std::size_t *size, std::size_t offset) -> Status
{
    CDB_TRY(file_seek(m_file, static_cast<long>(offset), SEEK_SET, nullptr));
    return file_read(m_file, out, size);
}

PosixEditor::~PosixEditor()
{
    (void)file_close(m_file);
}

auto PosixEditor::read(char *out, std::size_t *size, std::size_t offset) -> Status
{
    CDB_TRY(file_seek(m_file, static_cast<long>(offset), SEEK_SET, nullptr));
    return file_read(m_file, out, size);
}

auto PosixEditor::write(Slice in, std::size_t offset) -> Status
{
    CDB_TRY(file_seek(m_file, static_cast<long>(offset), SEEK_SET, nullptr));
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

PosixInfoLogger::~PosixInfoLogger()
{
    (void)file_close(m_file);
}

// Based off LevelDB.
auto PosixInfoLogger::logv(const char *fmt, ...) -> void
{
    std::va_list args;
    for (int iteration {}; iteration < 2; ++iteration) {
        va_start(args, fmt);
        const auto rc = std::vsnprintf(m_buffer.data(), m_buffer.size(), fmt, args);
        va_end(args);

        if (rc < 0) {
            break;
        }
        auto length = static_cast<std::size_t>(rc);

        if (length >= m_buffer.size() - 1) {
            // The message did not fit into the buffer.
            if (iteration == 0) {
                m_buffer.resize(length + 2);
                continue;
            }
        }

        // Add a newline if necessary.
        if (m_buffer[length - 1] != '\n') {
            m_buffer[length] = '\n';
            ++length;
        }

        CDB_EXPECT_LE(length, m_buffer.size());
        file_write(m_file, Slice {m_buffer.data(), length});
        break;
    }
    m_buffer.resize(kBufferSize);
}

auto EnvPosix::resize_file(const std::string &path, std::size_t size) -> Status
{
    return file_resize(path, size);
}

auto EnvPosix::rename_file(const std::string &old_path, const std::string &new_path) -> Status
{
    if (rename(old_path.c_str(), new_path.c_str())) {
        return errno_to_status();
    }
    return Status::ok();
}

auto EnvPosix::remove_file(const std::string &path) -> Status
{
    return file_remove(path);
}

auto EnvPosix::file_exists(const std::string &path) const -> Status
{
    if (struct stat st; stat(path.c_str(), &st)) {
        return Status::not_found("not found");
    }
    return Status::ok();
}

auto EnvPosix::file_size(const std::string &path, std::size_t *out) const -> Status
{
    struct stat st;
    if (stat(path.c_str(), &st)) {
        return errno_to_status();
    }
    *out = static_cast<std::size_t>(st.st_size);
    return Status::ok();
}

auto EnvPosix::get_children(const std::string &path, std::vector<std::string> *out) const -> Status
{
    const auto skip = [](const auto *s) {
        return !std::strcmp(s, ".") || !std::strcmp(s, "..");
    };

    const auto [dir_path, base_path] = split_path(path);
    auto *dir = opendir(dir_path.c_str());
    if (dir == nullptr) {
        return errno_to_status();
    }
    struct dirent *ent;
    while ((ent = readdir(dir))) {
        if (skip(ent->d_name)) {
            continue;
        }
        out->emplace_back(ent->d_name);
    }
    closedir(dir);
    return Status::ok();
}

auto EnvPosix::new_reader(const std::string &path, Reader **out) -> Status
{
    int file;
    CDB_TRY(file_open(path, O_RDONLY, kFilePermissions, &file));
    *out = new PosixReader {path, file};
    return Status::ok();
}

auto EnvPosix::new_editor(const std::string &path, Editor **out) -> Status
{
    int file;
    CDB_TRY(file_open(path, O_CREAT | O_RDWR, kFilePermissions, &file));
    *out = new PosixEditor {path, file};
    return Status::ok();
}

auto EnvPosix::new_logger(const std::string &path, Logger **out) -> Status
{
    int file;
    CDB_TRY(file_open(path, O_CREAT | O_WRONLY | O_APPEND, kFilePermissions, &file));
    *out = new PosixLogger {path, file};
    return Status::ok();
}

auto EnvPosix::new_info_logger(const std::string &path, InfoLogger **out) -> Status
{
    int file;
    CDB_TRY(file_open(path, O_CREAT | O_WRONLY | O_APPEND, kFilePermissions, &file));
    *out = new PosixInfoLogger {path, file};
    return Status::ok();
}

auto split_path(const std::string &filename) -> std::pair<std::string, std::string>
{
    auto *buffer = new char[filename.size() + 1]();

    std::strcpy(buffer, filename.c_str());
    std::string base {basename(buffer)};

    std::strcpy(buffer, filename.c_str());
    std::string dir {dirname(buffer)};

    delete[] buffer;

    return {dir, base};
}

auto join_paths(const std::string &lhs, const std::string &rhs) -> std::string
{
    return lhs + '/' + rhs;
}

} // namespace calicodb