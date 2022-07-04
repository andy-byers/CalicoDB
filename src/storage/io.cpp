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

} // calico