#include "posix_system.h"

#include <fcntl.h>
#include <filesystem>
#include <sys/stat.h>
#include <unistd.h>

#include "utils/expect.h"
#include "utils/utils.h"

namespace Calico::Posix {

namespace fs = std::filesystem;

auto error() -> Status
{
    return error(std::errc {std::exchange(errno, SUCCESS)});
}

auto error(std::errc code) -> Status
{
    return system_error(std::make_error_code(code).message());
}

auto file_exists(const std::string &name) -> Status
{
    std::error_code code;
    auto was_found = fs::exists(name, code);
    if (code) {
        return error(std::errc {code.value()});
    } else if (!was_found) {
        return not_found("cannot find file \"{}\"", name);
    }
    return ok();
}

auto file_open(const std::string &name, int mode, int permissions) -> tl::expected<int, Status>
{
    if (const auto fd = ::open(name.c_str(), mode, permissions); fd != FAILURE)
        return fd;
    // Special case for read-only files that don't exist.
    if (std::exchange(errno, SUCCESS) == ENOENT)
        return tl::make_unexpected(not_found(
            "could not open file: no such file or directory \"{}\"", name));
    return tl::make_unexpected(error());
}

auto file_close(int fd) -> Status
{
    if (::close(fd) == FAILURE)
        return error();
    return ok();
}

auto file_size(const std::string &path) -> tl::expected<Size, Status>
{
    std::error_code code;
    const auto size = fs::file_size(path, code);
    if (code) return tl::make_unexpected(system_error(code.message()));
    return size;
}

auto file_read(int file, Byte *out, Size size) -> tl::expected<Size, Status>
{
    auto remaining = size;
    for (Size i {}; remaining && i < size; ++i) {
        if (const auto n = ::read(file, out, remaining); n != FAILURE) {
            remaining -= static_cast<Size>(n);
            out += n;
        } else if (errno != EINTR) {
            return tl::make_unexpected(error());
        }
    }
    return size - remaining;
}

auto file_write(int file, Slice in) -> tl::expected<Size, Status>
{
    const auto target_size = in.size();

    for (Size i {}; !in.is_empty() && i < target_size; ++i) {
        if (const auto n = ::write(file, in.data(), in.size()); n != FAILURE) {
            in.advance(static_cast<Size>(n));
        } else if (errno != EINTR) {
            return tl::make_unexpected(error());
        }
    }
    return target_size - in.size();
}

auto file_sync(int fd) -> Status
{
    if (fsync(fd) == FAILURE)
        return error();
    return ok();
}

auto file_seek(int fd, long offset, int whence) -> tl::expected<Size, Status>
{
    if (const auto position = lseek(fd, offset, whence); position != FAILURE)
        return static_cast<Size>(position);
    return tl::make_unexpected(error());
}

auto file_remove(const std::string &path) -> Status
{
    if (::unlink(path.c_str()) == FAILURE)
        return error();
    return ok();
}

auto file_resize(const std::string &path, Size size) -> Status
{
    std::error_code code;
    fs::resize_file(path, size, code);
    if (code) return system_error(code.message());
    return ok();
}

auto dir_create(const std::string &path, mode_t permissions) -> Status
{
    if (::mkdir(path.c_str(), permissions) == FAILURE) {
        if (std::exchange(errno, SUCCESS) == EEXIST)
            return logic_error("could not create directory: directory {} already exists", path);
        return error();
    }
    return ok();
}

auto dir_remove(const std::string &path) -> Status
{
    if (::rmdir(path.c_str()) == FAILURE)
        return error();
    return ok();
}

} // namespace Calico::Posix
