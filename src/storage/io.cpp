#include "io.h"
#include <filesystem>
#include "file.h"
#include "system.h"
#include "utils/expect.h"

namespace calico {

namespace fs = std::filesystem;

FileReader::FileReader(File &file)
    : m_file {&file}
{
    CALICO_EXPECT_TRUE(file.is_open());
    CALICO_EXPECT_TRUE(file.is_readable());
}

auto FileReader::seek(long offset, Seek whence) -> void
{
    system::seek(m_file->file(), offset, static_cast<int>(whence));
}

auto FileReader::read(Bytes out) -> Size
{
    return system::read(m_file->file(), out);
}

auto FileReader::read_at(Bytes out, Index offset) -> Size
{
    seek(static_cast<long>(offset), Seek::BEGIN);
    return read(out);
}






auto FileReader::noex_seek(long offset, Seek whence) -> Result<Index>
{
    return system::noex_seek(m_file->file(), offset, static_cast<int>(whence));
}

auto FileReader::noex_read(Bytes out) -> Result<Size>
{
    return system::noex_read(m_file->file(), out);
}

auto FileReader::noex_read_at(Bytes out, Index offset) -> Result<Size>
{
    return noex_seek(static_cast<long>(offset), Seek::BEGIN)
        .and_then([out, this](Index) -> Result<Size> {
            return noex_read(out);
        });
}

FileWriter::FileWriter(File &file)
    : m_file {&file}
{
    CALICO_EXPECT_TRUE(file.is_open());
    CALICO_EXPECT_TRUE(file.is_writable());
}

auto FileWriter::seek(long offset, Seek whence) -> void
{
    system::seek(m_file->file(), offset, static_cast<int>(whence));
}

auto FileWriter::write(BytesView in) -> Size
{
    return system::write(m_file->file(), in);
}

auto FileWriter::write_at(BytesView in, Index offset) -> Size
{
    seek(static_cast<long>(offset), Seek::BEGIN);
    return write(in);
}

auto FileWriter::sync() -> void
{
    system::sync(m_file->file());
}

auto FileWriter::resize(Size size) -> void
{
    fs::resize_file(m_file->path(), size);
}









auto FileWriter::noex_seek(long offset, Seek whence) -> Result<Index>
{
    return system::noex_seek(m_file->file(), offset, static_cast<int>(whence));
}

auto FileWriter::noex_write(BytesView in) -> Result<Size>
{
    return system::noex_write(m_file->file(), in);
}

auto FileWriter::noex_write_at(BytesView in, Index offset) -> Result<Size>
{
    return noex_seek(static_cast<long>(offset), Seek::BEGIN)
        .and_then([in, this](Index) -> Result<Size> {
            return noex_write(in);
        });
}

auto FileWriter::noex_sync() -> Result<void>
{
    return system::noex_sync(m_file->file());
}

auto FileWriter::noex_resize(Size size) -> Result<void>
{
    std::error_code error;
    fs::resize_file(m_file->path(), size, error);
    if (error)
        return ErrorResult {Error::system_error(error.message())};
    return {};
}

} // calico