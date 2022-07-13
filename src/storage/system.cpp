#include "system.h"
#include <fcntl.h>
#include <unistd.h>

namespace calico::system {

auto error() -> Error
{
    return error(std::errc {std::exchange(errno, SUCCESS)});
}

auto error(std::errc code) -> Error
{
    return Error::system_error(std::make_error_code(code).message());
}

auto open(const std::string &name, int mode, int permissions) -> Result<int>
{
    if (const auto fd = ::open(name.c_str(), mode, permissions); fd != FAILURE)
        return fd;
    return ErrorResult {error()};
}

auto close(int fd) -> Result<void>
{
    if (::close(fd) == FAILURE)
        return ErrorResult {error()};
    return {};
}

auto read(int file, Bytes out) -> Result<Size>
{
    const auto target_size = out.size();

    for (Index i {}; !out.is_empty() && i < target_size; ++i) {
        if (const auto n = ::read(file, out.data(), out.size()); n != FAILURE) {
            out.advance(static_cast<Size>(n));
        } else if (errno != EINTR) {
            return ErrorResult {error()};
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
            return ErrorResult {error()};
        }
    }
    return target_size - in.size();
}

auto sync(int fd) -> Result<void>
{
    if (fsync(fd) == FAILURE)
        return ErrorResult {error()};
    return {};
}

auto seek(int fd, long offset, int whence) -> Result<Index>
{
    if (const auto position = lseek(fd, offset, whence); position != FAILURE)
        return static_cast<Index>(position);
    return ErrorResult {error()};
}

auto unlink(const std::string &path) -> Result<void>
{
    if (::unlink(path.c_str()) == FAILURE)
        return ErrorResult {error()};
    return {};
}

} // calico::system
