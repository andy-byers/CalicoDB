#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "exception.h"
#include "system.h"

namespace cub::system {

auto use_direct_io([[maybe_unused]] int fd) -> void
{
#if defined(CUB_OSX) && !CUB_HAS_O_DIRECT
    // Turn off kernel page caching. OSX (at least my platform) doesn't use O_DIRECT but provides
    // an API for turning off page caching via fcntl().
    if (fcntl(fd, F_NOCACHE) == FAILURE)
        throw SystemError {"fcntl"};
#endif
}

// TODO
auto access(const std::string &name, int mode) -> bool
{
    return ::access(name.c_str(), mode) == 0;
}

auto open(const std::string &name, int mode, int permissions) -> int
{
    if (const auto fd = ::open(name.c_str(), mode, permissions); fd != FAILURE)
        return fd;
    throw SystemError {"open"};
}

auto close(int fd) -> void
{
    if (::close(fd) == FAILURE)
        throw SystemError {"close"};
}

auto unlink(const std::string &name) -> void
{
    if (::unlink(name.c_str()) == FAILURE)
        throw SystemError {"unlink"};
}

auto rename(const std::string &old_name, const std::string &new_name) -> void
{
    if (::rename(old_name.c_str(), new_name.c_str()) == FAILURE)
        throw SystemError {"rename"};
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
    throw SystemError {"read"};
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
    throw SystemError {"write"};
}

auto sync(int fd) -> void
{
    if (fsync(fd) == FAILURE)
        throw SystemError {"fsync"};
}

auto seek(int fd, long offset, int whence) -> Index
{
    if (const auto position = lseek(fd, offset, whence); position != FAILURE)
        return static_cast<Index>(position);
    throw SystemError {"lseek"};
}

auto size(int file) -> Size
{
    if (struct stat st {}; fstat(file, &st) != FAILURE)
        return static_cast<Size>(st.st_size);
    throw SystemError {"fstat"};
}

// TODO
auto exists(const std::string &path) -> bool
{
    struct stat unused {};
    return stat(path.c_str(), &unused) == SUCCESS;
}

auto resize(int file, Size size) -> void
{
    if (ftruncate(file, static_cast<long>(size)) == FAILURE)
        throw SystemError {"ftruncate"};
}

} // cub::system
