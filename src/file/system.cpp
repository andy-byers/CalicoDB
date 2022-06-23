#include "system.h"
#include <filesystem>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "calico/exception.h"

namespace calico::system {

auto use_direct_io([[maybe_unused]] int fd) -> void
{
#if defined(CALICO_OSX) && !CALICO_HAS_O_DIRECT
    // Turn off kernel page caching. OSX (at least my platform) doesn't use O_DIRECT but provides
    // an API for turning off page caching via fcntl().
    if (fcntl(fd, F_NOCACHE) == FAILURE)
        throw std::system_error {errno, std::system_category(), "fcntl"};
#endif
}

auto exists(const std::string &path) -> bool
{
    return std::filesystem::exists(path);
}

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

auto unlink(const std::string &name) -> void
{
    if (::unlink(name.c_str()) == FAILURE)
        throw std::system_error {errno, std::system_category(), "unlink"};
}

auto rename(const std::string &old_name, const std::string &new_name) -> void
{
    if (::rename(old_name.c_str(), new_name.c_str()) == FAILURE)
        throw std::system_error {errno, std::system_category(), "rename"};
}

auto read(int fd, Bytes data) -> Size
{
    for (Index i {}; i < data.size(); ++i) {
        if (const auto read_size = ::read(fd, data.data(), data.size()); read_size != FAILURE) {
            return static_cast<Size>(read_size);
        } else if (errno != EINTR) {
            break;
        }
    }
    throw std::system_error {errno, std::system_category(), "read"};
}

auto write(int fd, BytesView data) -> Size
{
    for (Index i {}; i < data.size(); ++i) {
        if (const auto write_size = ::write(fd, data.data(), data.size()); write_size != FAILURE) {
            return static_cast<Size>(write_size);
        } else if (errno != EINTR) {
            break;
        }
    }
    throw std::system_error {errno, std::system_category(), "write"};
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

auto size(int file) -> Size
{
    if (struct stat st {}; fstat(file, &st) != FAILURE)
        return static_cast<Size>(st.st_size);
    throw std::system_error {errno, std::system_category(), "fstat"};
}

auto resize(int file, Size size) -> void
{
    if (ftruncate(file, static_cast<long>(size)) == FAILURE)
        throw std::system_error {errno, std::system_category(), "ftruncate"};
}

} // calico::system
