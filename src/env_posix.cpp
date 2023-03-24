// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "env_posix.h"
#include "logging.h"
#include "utils.h"
#include <cstdarg>
#include <dirent.h>
#include <fcntl.h>
#include <libgen.h>
#include <sys/stat.h>
#include <unistd.h>

namespace calicodb
{

static constexpr int kFilePermissions = 0644; // -rw-r--r--

[[nodiscard]] static auto to_status(int code) -> Status
{
    if (code == ENOENT) {
        return Status::not_found(strerror(code));
    } else {
        return Status::io_error(strerror(code));
    }
}

[[nodiscard]] static auto errno_to_status() -> Status
{
    const auto code = errno;
    errno = 0;
    return to_status(code);
}

[[nodiscard]] static auto file_open(const std::string &name, int mode, int permissions, int &out) -> Status
{
    if (const auto fd = open(name.c_str(), mode, permissions); fd >= 0) {
        out = fd;
        return Status::ok();
    }
    out = -1;
    return errno_to_status();
}

[[nodiscard]] static auto file_read(int file, std::size_t size, char *scratch, Slice *out) -> Status
{
    for (;;) {
        const auto n = read(file, scratch, size);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return errno_to_status();
        }
        if (out != nullptr) {
            *out = Slice(scratch, static_cast<std::size_t>(n));
        }
        break;
    }
    return Status::ok();
}

[[nodiscard]] static auto file_write(int file, Slice in) -> Status
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

[[nodiscard]] static auto file_sync(int fd) -> Status
{
    if (fsync(fd)) {
        return errno_to_status();
    }
    return Status::ok();
}

[[nodiscard]] static auto file_seek(int fd, long offset, int whence, std::size_t *out) -> Status
{
    if (const auto position = lseek(fd, offset, whence); position != -1) {
        if (out) {
            *out = static_cast<std::size_t>(position);
        }
        return Status::ok();
    }
    return errno_to_status();
}

[[nodiscard]] static auto file_remove(const std::string &filename) -> Status
{
    if (unlink(filename.c_str())) {
        return errno_to_status();
    }
    return Status::ok();
}

[[nodiscard]] static auto file_resize(const std::string &filename, std::size_t size) -> Status
{
    if (truncate(filename.c_str(), static_cast<off_t>(size))) {
        return errno_to_status();
    }
    return Status::ok();
}

PosixReader::PosixReader(std::string filename, int file)
    : m_filename {std::move(filename)},
      m_file {file}
{
    CALICODB_EXPECT_GE(file, 0);
}

PosixReader::~PosixReader()
{
    close(m_file);
}

auto PosixReader::read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status
{
    CALICODB_TRY(file_seek(m_file, static_cast<long>(offset), SEEK_SET, nullptr));
    return file_read(m_file, size, scratch, out);
}

PosixEditor::PosixEditor(std::string filename, int file)
    : m_filename {std::move(filename)},
      m_file {file}
{
    CALICODB_EXPECT_GE(file, 0);
}

PosixEditor::~PosixEditor()
{
    close(m_file);
}

auto PosixEditor::read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status
{
    CALICODB_TRY(file_seek(m_file, static_cast<long>(offset), SEEK_SET, nullptr));
    return file_read(m_file, size, scratch, out);
}

auto PosixEditor::write(std::size_t offset, const Slice &in) -> Status
{
    CALICODB_TRY(file_seek(m_file, static_cast<long>(offset), SEEK_SET, nullptr));
    return file_write(m_file, in);
}

auto PosixEditor::sync() -> Status
{
    return file_sync(m_file);
}

PosixLogger::PosixLogger(std::string filename, int file)
    : m_filename {std::move(filename)},
      m_file {file}
{
    CALICODB_EXPECT_GE(file, 0);
}

PosixLogger::~PosixLogger()
{
    close(m_file);
}

auto PosixLogger::write(const Slice &in) -> Status
{
    return file_write(m_file, in);
}

auto PosixLogger::sync() -> Status
{
    return file_sync(m_file);
}

PosixInfoLogger::PosixInfoLogger(std::string filename, int file)
    : m_buffer(kBufferSize, '\0'),
      m_filename {std::move(filename)},
      m_file {file}
{
    CALICODB_EXPECT_GE(file, 0);
}

PosixInfoLogger::~PosixInfoLogger()
{
    close(m_file);
}

auto PosixInfoLogger::logv(const char *fmt, ...) -> void
{
    std::va_list args;
    va_start(args, fmt);
    const auto length = write_to_string(m_buffer, fmt, args);
    va_end(args);

    (void)file_write(m_file, Slice(m_buffer.data(), length));
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

auto EnvPosix::file_exists(const std::string &path) const -> bool
{
    return access(path.c_str(), F_OK) == 0;
}

auto EnvPosix::file_size(const std::string &path, std::size_t &out) const -> Status
{
    struct stat st;
    if (stat(path.c_str(), &st)) {
        return errno_to_status();
    }
    out = static_cast<std::size_t>(st.st_size);
    return Status::ok();
}

auto EnvPosix::get_children(const std::string &dirname, std::vector<std::string> &out) const -> Status
{
    const auto skip = [](const auto *s) {
        return !std::strcmp(s, ".") || !std::strcmp(s, "..");
    };

    out.clear();
    auto *dir = opendir(dirname.c_str());
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

auto EnvPosix::sync_directory(const std::string &dirname) -> Status
{
    int dir;
    CALICODB_TRY(file_open(dirname, O_RDONLY, kFilePermissions, dir));
    auto s = file_sync(dir);
    close(dir);
    return s;
}

auto EnvPosix::new_reader(const std::string &filename, Reader *&out) -> Status
{
    int file;
    CALICODB_TRY(file_open(filename, O_RDONLY, kFilePermissions, file));
    out = new PosixReader(filename, file);
    return Status::ok();
}

auto EnvPosix::new_editor(const std::string &filename, Editor *&out) -> Status
{
    int file;
    CALICODB_TRY(file_open(filename, O_CREAT | O_RDWR, kFilePermissions, file));
    out = new PosixEditor(filename, file);
    return Status::ok();
}

auto EnvPosix::new_logger(const std::string &filename, Logger *&out) -> Status
{
    int file;
    CALICODB_TRY(file_open(filename, O_CREAT | O_WRONLY | O_APPEND, kFilePermissions, file));
    out = new PosixLogger(filename, file);
    return Status::ok();
}

auto EnvPosix::new_info_logger(const std::string &filename, InfoLogger *&out) -> Status
{
    int file;
    CALICODB_TRY(file_open(filename, O_CREAT | O_WRONLY | O_APPEND, kFilePermissions, file));
    out = new PosixInfoLogger(filename, file);
    return Status::ok();
}

auto split_path(const std::string &filename) -> std::pair<std::string, std::string>
{
    auto *buffer = new char[filename.size() + 1]();

    std::strcpy(buffer, filename.c_str());
    std::string base(basename(buffer));

    std::strcpy(buffer, filename.c_str());
    std::string dir(dirname(buffer));

    delete[] buffer;

    return {dir, base};
}

auto join_paths(const std::string &lhs, const std::string &rhs) -> std::string
{
    return lhs + '/' + rhs;
}

} // namespace calicodb