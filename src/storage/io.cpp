#include "io.h"
#include <filesystem>
#include "file.h"
#include "system.h"
#include "utils/expect.h"

namespace cco {

namespace fs = std::filesystem;

FileReader::FileReader(File &file)
    : m_file {&file}
{
    CCO_EXPECT_TRUE(file.is_open());
}

auto FileReader::seek(long offset, Seek whence) -> Result<Index>
{
    return system::seek(m_file->file(), offset, static_cast<int>(whence));
}

auto FileReader::read(Bytes out) -> Result<Size>
{
    return system::read(m_file->file(), out);
}

auto FileReader::read(Bytes out, Index offset) -> Result<Size>
{
    return seek(static_cast<long>(offset), Seek::BEGIN)
        .and_then([out, this](Index) -> Result<Size> {
            return read(out);
        });
}

FileWriter::FileWriter(File &file)
    : m_file {&file}
{
    CCO_EXPECT_TRUE(file.is_open());
}

auto FileWriter::seek(long offset, Seek whence) -> Result<Index>
{
    return system::seek(m_file->file(), offset, static_cast<int>(whence));
}

auto FileWriter::write(BytesView in) -> Result<Size>
{
    return system::write(m_file->file(), in);
}

auto FileWriter::write(BytesView in, Index offset) -> Result<Size>
{
    return seek(static_cast<long>(offset), Seek::BEGIN)
        .and_then([in, this](Index) -> Result<Size> {
            return write(in);
        });
}

auto FileWriter::sync() -> Result<void>
{
    return system::sync(m_file->file());
}

auto FileWriter::resize(Size size) -> Result<void>
{
    std::error_code error;
    fs::resize_file(m_file->path(), size, error);
    if (error)
        return Err {Error::system_error(error.message())};
    return {};
}

} // calico