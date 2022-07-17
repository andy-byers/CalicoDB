#include "file.h"
#include "system.h"
#include "utils/expect.h"

namespace cco {

namespace fs = std::filesystem;

auto File::is_open() const -> bool
{
    CCO_EXPECT_GE(m_file, system::FAILURE);
    return m_file != system::FAILURE;
}

auto File::mode() const -> Mode
{
    return m_mode;
}

auto File::permissions() const -> int
{
    return m_permissions;
}

auto File::path() const -> std::string
{
    return m_path;
}

auto File::name() const -> std::string
{
    return m_path.filename();
}

auto File::file() const -> int
{
    return m_file;
}

auto File::size() const -> Result<Size>
{
    static constexpr auto INVALID_SIZE = static_cast<std::uintmax_t>(-1);

    std::error_code error;
    if (const auto size = fs::file_size(m_path, error); size != INVALID_SIZE)
        return size;
    return Err {Status::system_error(error.message())};
}

auto File::open(const std::string &path, Mode mode, int permissions) -> Result<void>
{
    return system::open(path, static_cast<int>(mode), permissions)
        .and_then([&path, mode, permissions, this](int fd) -> Result<void> {
            m_file = fd;
            m_path = path;
            m_mode = mode;
            m_permissions = permissions;
            return {};
        });
}

auto File::close() -> Result<void>
{
    return system::close(m_file)
        .and_then([this]() -> Result<void> {
            m_file = system::FAILURE;
            return {};
        });
}

auto File::rename(const std::string &name) -> Result<void>
{
    CCO_EXPECT_TRUE(m_path.has_parent_path());
    CCO_EXPECT_FALSE(m_path.empty());
    std::error_code error;
    fs::rename(std::exchange(m_path, m_path.parent_path() / name), m_path, error);
    if (error)
        return Err {Status::system_error(error.message())};
    return {};
}

auto File::remove() -> Result<void>
{
    return system::unlink(m_path)
        .and_then([this]() -> Result<void> {
            m_path.clear();
            return {};
        });
}

auto File::seek(long offset, Seek whence) -> Result<Index>
{
    return system::seek(m_file, offset, static_cast<int>(whence));
}

auto File::read(Bytes out) -> Result<Size>
{
    return system::read(m_file, out);
}

auto File::read(Bytes out, Index offset) -> Result<Size>
{
    return seek(static_cast<long>(offset), Seek::BEGIN)
        .and_then([out, this](Index) -> Result<Size> {
            return read(out);
        });
}

auto File::write(BytesView in) -> Result<Size>
{
    return system::write(m_file, in);
}

auto File::write(BytesView in, Index offset) -> Result<Size>
{
    return seek(static_cast<long>(offset), Seek::BEGIN)
        .and_then([in, this](Index) -> Result<Size> {
            return write(in);
        });
}

auto File::sync() -> Result<void>
{
    return system::sync(m_file);
}

auto File::resize(Size size) -> Result<void>
{
    std::error_code error;
    fs::resize_file(m_path, size, error);
    if (error)
        return Err {Status::system_error(error.message())};
    return {};
}

auto read_exact(IFile &file, Bytes in) -> Result<void>
{
    return file.read(in)
        .and_then([in](Size read_size) {
            return read_size == in.size()
                ? Result<void> {}
                : Err {system::error(std::errc::io_error)};
        });
}

auto read_exact_at(IFile &file, Bytes in, Index offset) -> Result<void>
{
    return file.read(in, offset)
        .and_then([in](Size read_size) {
            return read_size == in.size() ? Result<void> {} : Err {system::error(std::errc::io_error)};
        });
}

auto write_all(IFile &file, BytesView out) -> Result<void>
{
    return file.write(out)
        .and_then([out](Size write_size) {
            return write_size == out.size() ? Result<void> {} : Err {system::error(std::errc::io_error)};
        });
}

auto write_all(IFile &file, BytesView out, Index offset) -> Result<void>
{
    return file.write(out, offset)
        .and_then([out](Size write_size) {
            return write_size == out.size() ? Result<void> {} : Err {system::error(std::errc::io_error)};
        });
}

} // cco
