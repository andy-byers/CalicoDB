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

auto File::name() const -> std::string
{
    return m_name;
}

auto File::file() const -> int
{
    return m_file;
}

auto File::size() const -> Result<Size>
{
    // NOTE: This will mess up the file cursor!
    return system::size(m_file);
}

auto File::close() -> Result<void>
{
    return system::close(m_file)
        .and_then([this]() -> Result<void> {
            m_file = system::FAILURE;
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
    fs::resize_file(m_name, size, error);
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

} // namespace cco
