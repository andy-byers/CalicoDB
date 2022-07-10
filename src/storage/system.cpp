#include "system.h"
#include <fcntl.h>
#include <unistd.h>
#include "utils/expect.h"

namespace calico::system {

auto open(const std::string &name, int mode, int permissions) -> int
{
    if (const auto fd = ::open(name.c_str(), mode, permissions); fd != FAILURE)
        return fd;
    throw std::system_error {errno, std::system_category(), "open"};
}

auto close(int fd) -> void
{
    if (::close(fd) == FAILURE)
        throw std::system_error {errno, std::system_category(), "close"};
}

auto read(int file, Bytes out) -> Size
{
    const auto target_size = out.size();

    // Try to read target_size times. We retry on partial reads, so we can read as little as
    // one byte per iteration. Also note that system::read() already ignores EINTR some number
    // of times before failing. This is so that we don't generate exceptions unless something
    // really goes wrong.
    for (Index i {}; !out.is_empty() && i < target_size; ++i) {
        if (const auto n = ::read(file, out.data(), out.size()); n != FAILURE) {
            out.advance(static_cast<Size>(n));
        } else if (errno != EINTR) {
            throw std::system_error {errno, std::system_category(), "read"};
        }
    }
    return target_size - out.size();
}

auto write(int file, BytesView in) -> Size
{
    const auto target_size = in.size();

    for (Index i {}; !in.is_empty() && i < target_size; ++i) {
        if (const auto n = ::write(file, in.data(), in.size()); n != system::FAILURE) {
            in.advance(static_cast<Size>(n));
        } else if (errno != EINTR) {
            throw std::system_error {errno, std::system_category(), "write"};
        }
    }
    return target_size - in.size();
}

auto sync(int fd) -> void
{
    if (fsync(fd) == FAILURE)
        throw std::system_error {errno, std::system_category(), "fsync"};
}

auto seek(int fd, long offset, int whence) -> Index
{
    if (const auto position = lseek(fd, offset, whence); position != FAILURE)
        return static_cast<Index>(position);
    throw std::system_error {errno, std::system_category(), "lseek"};
}








auto error() -> Error
{
    return error(std::errc {std::exchange(errno, SUCCESS)});
}

auto error(std::errc code) -> Error
{
    return Error::system_error(std::make_error_code(code).message());
}

auto noex_open(const std::string &name, int mode, int permissions) -> Result<int>
{
    if (const auto fd = ::open(name.c_str(), mode, permissions); fd != FAILURE)
        return fd;
    return ErrorResult {error()};
}

auto noex_close(int fd) -> Result<void>
{
    if (::close(fd) == FAILURE)
        return ErrorResult {error()};
    return {};
}

auto noex_read(int file, Bytes out) -> Result<Size>
{
    const auto target_size = out.size();

    // Try to read target_size times. We retry on partial reads, so we can read as little as
    // one byte per iteration. Also note that system::read() already ignores EINTR some number
    // of times before failing. This is so that we don't generate exceptions unless something
    // really goes wrong.
    for (Index i {}; !out.is_empty() && i < target_size; ++i) {
        if (const auto n = ::read(file, out.data(), out.size()); n != FAILURE) {
            out.advance(static_cast<Size>(n));
        } else if (errno != EINTR) {
            return ErrorResult {error()};
        }
    }
    return target_size - out.size();
}

auto noex_write(int file, BytesView in) -> Result<Size>
{
    const auto target_size = in.size();

    for (Index i {}; !in.is_empty() && i < target_size; ++i) {
        if (const auto n = ::write(file, in.data(), in.size()); n != system::FAILURE) {
            in.advance(static_cast<Size>(n));
        } else if (errno != EINTR) {
            return ErrorResult {error()};
        }
    }
    return target_size - in.size();
}

auto noex_sync(int fd) -> Result<void>
{
    if (fsync(fd) == FAILURE)
        return ErrorResult {error()};
    return {};
}

auto noex_seek(int fd, long offset, int whence) -> Result<Index>
{
    if (const auto position = lseek(fd, offset, whence); position != FAILURE)
        return static_cast<Index>(position);
    return ErrorResult {error()};
}

auto noex_unlink(const std::string &path) -> Result<void>
{
    CALICO_EXPECT_FALSE(path.empty());
    if (unlink(path.c_str()) == FAILURE)
        return ErrorResult {error()};
    return {};
}

} // calico::system
