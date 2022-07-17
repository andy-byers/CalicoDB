#include "system.h"
#include <filesystem>
#include <fcntl.h>
#include <unistd.h>

namespace cco::system {

namespace fs = std::filesystem;

auto error() -> Status
{
    return error(std::errc {std::exchange(errno, SUCCESS)});
}

auto error(std::errc code) -> Status
{
    return Status::system_error(std::make_error_code(code).message());
}

auto exists(const std::string &name) -> Result<bool>
{
    std::error_code code;
    auto was_found = fs::exists(name, code);
    if (code)
        return Err {error(std::errc {code.value()})};
    return was_found;
}

auto open(const std::string &name, int mode, int permissions) -> Result<int>
{
    if (const auto fd = ::open(name.c_str(), mode, permissions); fd != FAILURE)
        return fd;
    return Err {error()};
}

auto close(int fd) -> Result<void>
{
    if (::close(fd) == FAILURE)
        return Err {error()};
    return {};
}

auto read(int file, Bytes out) -> Result<Size>
{
    const auto target_size = out.size();

    for (Index i {}; !out.is_empty() && i < target_size; ++i) {
        if (const auto n = ::read(file, out.data(), out.size()); n != FAILURE) {
            out.advance(static_cast<Size>(n));
        } else if (errno != EINTR) {
            return Err {error()};
        }
    }
    return target_size - out.size();
}

auto write(int file, BytesView in) -> Result<Size>
{
    const auto target_size = in.size();

    for (Index i {}; !in.is_empty() && i < target_size; ++i) {
        if (const auto n = ::write(file, in.data(), in.size()); n != FAILURE) {
            in.advance(static_cast<Size>(n));
        } else if (errno != EINTR) {
            return Err {error()};
        }
    }
    return target_size - in.size();
}

auto sync(int fd) -> Result<void>
{
    if (fsync(fd) == FAILURE)
        return Err {error()};
    return {};
}

auto seek(int fd, long offset, int whence) -> Result<Index>
{
    if (const auto position = lseek(fd, offset, whence); position != FAILURE)
        return static_cast<Index>(position);
    return Err {error()};
}

auto unlink(const std::string &path) -> Result<void>
{
    if (::unlink(path.c_str()) == FAILURE)
        return Err {error()};
    return {};
}

} // cco::system
