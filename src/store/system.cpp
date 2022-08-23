#include "system.h"
#include "utils/expect.h"
#include "utils/logging.h"
#include <fcntl.h>
#include <filesystem>
#include <sys/stat.h>
#include <unistd.h>

namespace calico::system {

namespace fs = std::filesystem;

auto error() -> Status
{
    return error(std::errc {std::exchange(errno, SUCCESS)});
}

auto error(std::errc code) -> Status
{
    return error(std::make_error_code(code).message());
}

auto error(const std::string &message) -> Status
{
    return Status::system_error(message);
}

auto file_exists(const std::string &name) -> Status
{
    std::error_code code;
    auto was_found = fs::exists(name, code);
    if (code) {
        return error(std::errc {code.value()});
    } else if (!was_found) {
        const auto message = fmt::format("cannot find file \"{}\"", name);
        return Status::not_found(message);
    }
    return Status::ok();
}

auto file_open(const std::string &name, int mode, int permissions) -> Result<int>
{
    if (const auto fd = ::open(name.c_str(), mode, permissions); fd != FAILURE)
        return fd;
    if (std::exchange(errno, SUCCESS) == ENOENT) {
        // Special case for read-only files that don't exist.
        ThreePartMessage message;
        message.set_primary("could not open file");
        message.set_detail("no such file or directory \"{}\"", name);
        return Err {message.not_found()};
    }
    return Err {error()};
}

auto file_close(int fd) -> Status
{
    if (::close(fd) == FAILURE)
        return error();
    return Status::ok();
}

auto file_size(const std::string &path) -> Result<Size>
{
    std::error_code code;
    const auto size = fs::file_size(path, code);
    if (code) return Err {Status::system_error(code.message())};
    return size;
}

auto file_read(int file, Bytes out) -> Result<Size>
{
    const auto target_size = out.size();

    for (Size i {}; !out.is_empty() && i < target_size; ++i) {
        if (const auto n = ::read(file, out.data(), out.size()); n != FAILURE) {
            out.advance(static_cast<Size>(n));
        } else if (errno != EINTR) {
            return Err {error()};
        }
    }
    return target_size - out.size();
}

auto file_write(int file, BytesView in) -> Result<Size>
{
    const auto target_size = in.size();

    for (Size i {}; !in.is_empty() && i < target_size; ++i) {
        if (const auto n = ::write(file, in.data(), in.size()); n != FAILURE) {
            in.advance(static_cast<Size>(n));
        } else if (errno != EINTR) {
            return Err {error()};
        }
    }
    return target_size - in.size();
}

auto file_sync(int fd) -> Status
{
    if (fsync(fd) == FAILURE)
        return error();
    return Status::ok();
}

auto file_seek(int fd, long offset, int whence) -> Result<Size>
{
    if (const auto position = lseek(fd, offset, whence); position != FAILURE)
        return static_cast<Size>(position);
    return Err {error()};
}

auto file_remove(const std::string &path) -> Status
{
    if (::unlink(path.c_str()) == FAILURE)
        return error();
    return Status::ok();
}

auto file_resize(const std::string &path, Size size) -> Status
{
    std::error_code code;
    fs::resize_file(path, size, code);
    if (code) return error(code.message());
    return Status::ok();
}

auto dir_create(const std::string &path, mode_t permissions) -> Status
{
    if (::mkdir(path.c_str(), permissions) == FAILURE) {
        if (std::exchange(errno, SUCCESS) == EEXIST) {
            ThreePartMessage message;
            message.set_primary("could not create directory");
            message.set_detail("directory {} already exists", path);
            return message.logic_error();
        }
        return error();
    }
    return Status::ok();
}

auto dir_remove(const std::string &path) -> Status
{
    if (::rmdir(path.c_str()) == FAILURE)
        return error();
    return Status::ok();
}

} // namespace calico::system
