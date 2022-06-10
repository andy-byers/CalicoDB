#include "file.h"

#include <fcntl.h>
#include "system.h"

namespace cub {

namespace {

    auto robust_read(const Resource &resource, Bytes out)
    {
        const auto target_size = out.size();

        // Try to read target_size times. We retry on partial reads, so we can read as little as
        // one byte per iteration. Also note that System::read_from_file() ignores EINTR a given
        // number of times before failing.
        for (size_t i{}; !out.is_empty() && i < target_size; ++i) {
            try {
                if (const auto n = system::read(resource.fd(), out)) {
                    out.advance(n);
                } else {
                    break;
                }
            } catch (const SystemError &error) {
                throw IOError{error};
            }
        }
        return target_size - out.size();
    }

    auto robust_write(const Resource &resource, BytesView in)
    {
        const auto target_size = in.size();

        // Same as File::read(), except that there is no concept of EOF.
        for (size_t i{}; !in.is_empty() && i < target_size; ++i) {
            try {
                if (const auto n = system::write(resource.fd(), in))
                    in.advance(n);
            } catch (const SystemError &error) {
                throw IOError{error};
            }
        }
        return target_size - in.size();
    }

    constexpr auto APPEND = O_APPEND;
    constexpr auto READ_ONLY = O_RDONLY;
    constexpr auto READ_WRITE = O_RDWR;
    constexpr auto WRITE_ONLY = O_WRONLY;

} // <anonymous>

Resource::Resource(const std::string &name, int type_bits, Mode mode, int permissions)
    : m_fd{system::open(name, type_bits | static_cast<int>(mode), permissions)} {}

Resource::~Resource()
{
    // TODO: Log.
    try {system::close(m_fd);} catch (...) {}
}

ReadOnlyFile::ReadOnlyFile(const std::string &path, Mode mode, int permissions)
    : m_resource{path, READ_ONLY, mode, permissions} {}

ReadOnlyFile::~ReadOnlyFile() = default;

auto ReadOnlyFile::size() const -> Size
{
    return system::size(m_resource.fd());
}

auto ReadOnlyFile::use_direct_io() -> void
{
    system::use_direct_io(m_resource.fd());
}

auto ReadOnlyFile::sync() -> void
{
    system::sync(m_resource.fd());
}

auto ReadOnlyFile::seek(long offset, Seek whence) -> Index
{
    return system::seek(m_resource.fd(), offset, static_cast<int>(whence));
}

auto ReadOnlyFile::read(Bytes out) -> Size
{
    return robust_read(m_resource, out);
}

WriteOnlyFile::WriteOnlyFile(const std::string &path, Mode mode, int permissions)
    : m_resource{path, WRITE_ONLY, mode, permissions} {}

WriteOnlyFile::~WriteOnlyFile() = default;

auto WriteOnlyFile::size() const -> Size
{
    return system::size(m_resource.fd());
}

auto WriteOnlyFile::use_direct_io() -> void
{
    system::use_direct_io(m_resource.fd());
}

auto WriteOnlyFile::sync() -> void
{
    system::sync(m_resource.fd());
}

auto WriteOnlyFile::resize(Size size) -> void
{
    system::resize(m_resource.fd(), size);
}

auto WriteOnlyFile::seek(long offset, Seek whence) -> Index
{
    return system::seek(m_resource.fd(), offset, static_cast<int>(whence));
}

auto WriteOnlyFile::write(BytesView in) -> Size
{
    return robust_write(m_resource, in);
}

ReadWriteFile::ReadWriteFile(const std::string &path, Mode mode, int permissions)
    : m_resource{path, READ_WRITE, mode, permissions} {}

ReadWriteFile::~ReadWriteFile() = default;

auto ReadWriteFile::size() const -> Size
{
    return system::size(m_resource.fd());
}

auto ReadWriteFile::use_direct_io() -> void
{
    system::use_direct_io(m_resource.fd());
}

auto ReadWriteFile::sync() -> void
{
    system::sync(m_resource.fd());
}

auto ReadWriteFile::resize(Size size) -> void
{
    system::resize(m_resource.fd(), size);
}

auto ReadWriteFile::seek(long offset, Seek whence) -> Index
{
    return system::seek(m_resource.fd(), offset, static_cast<int>(whence));
}

auto ReadWriteFile::read(Bytes out) -> Size
{
    return robust_read(m_resource, out);
}

auto ReadWriteFile::write(BytesView in) -> Size
{
    return robust_write(m_resource, in);
}

LogFile::LogFile(const std::string &path, Mode mode, int permissions)
    : m_resource{path, WRITE_ONLY | APPEND, mode, permissions} {}

LogFile::~LogFile() = default;

auto LogFile::size() const -> Size
{
    return system::size(m_resource.fd());
}

auto LogFile::use_direct_io() -> void
{
    system::use_direct_io(m_resource.fd());
}

auto LogFile::sync() -> void
{
    system::sync(m_resource.fd());
}

auto LogFile::resize(Size size) -> void
{
    system::resize(m_resource.fd(), size);
}

auto LogFile::write(BytesView in) -> Size
{
    return robust_write(m_resource, in);
}

} // cub