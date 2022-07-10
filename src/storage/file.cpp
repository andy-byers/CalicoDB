#include "file.h"
#include "io.h"
#include "system.h"
#include "utils/expect.h"

namespace calico {

namespace fs = std::filesystem;

auto File::is_open() const -> bool
{
    CALICO_EXPECT_GE(m_file, system::FAILURE);
    return m_file != system::FAILURE;
}

auto File::is_readable() const -> bool
{
    return (static_cast<unsigned>(m_mode) & static_cast<unsigned>(Mode::READ_ONLY)) == static_cast<unsigned>(Mode::READ_ONLY) ||
           (static_cast<unsigned>(m_mode) & static_cast<unsigned>(Mode::READ_WRITE)) == static_cast<unsigned>(Mode::READ_WRITE);
}

auto File::is_writable() const -> bool
{
    return (static_cast<unsigned>(m_mode) & static_cast<unsigned>(Mode::WRITE_ONLY)) == static_cast<unsigned>(Mode::WRITE_ONLY) ||
           (static_cast<unsigned>(m_mode) & static_cast<unsigned>(Mode::READ_WRITE)) == static_cast<unsigned>(Mode::READ_WRITE);
}

auto File::is_append() const -> bool
{
    return (static_cast<unsigned>(m_mode) & static_cast<unsigned>(Mode::APPEND)) == static_cast<unsigned>(Mode::APPEND);
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

auto File::size() const -> Size
{
    return fs::file_size(m_path);
}

auto File::file() const -> int
{
    return m_file;
}

auto File::open(const std::string &path, Mode mode, int permissions) -> void
{
    m_file = system::open(path, static_cast<int>(mode), permissions);
    m_path = path;
    m_mode = mode;
    m_permissions = permissions;
}

auto File::close() -> void
{
    system::close(m_file);
    m_file = system::FAILURE;
}

auto File::rename(const std::string &name) -> void
{
    CALICO_EXPECT_TRUE(m_path.has_parent_path());
    const auto old_path = std::exchange(m_path, m_path.parent_path() / name);
    fs::rename(old_path, m_path);
}

auto File::remove() -> void
{
    if (fs::remove(m_path))
        m_path.clear();
}

auto File::open_reader() -> std::unique_ptr<IFileReader>
{
    return std::make_unique<FileReader>(*this);
}

auto File::open_writer() -> std::unique_ptr<IFileWriter>
{
    return std::make_unique<FileWriter>(*this);
}

auto read_exact(IFileReader &reader, Bytes in) -> bool
{
    return reader.read(in) == in.size();
}

auto read_exact_at(IFileReader &reader, Bytes in, Index offset) -> bool
{
    return reader.read_at(in, offset) == in.size();
}

auto write_all(IFileWriter &writer, BytesView out) -> bool
{
    return writer.write(out) == out.size();
}

auto write_all_at(IFileWriter &writer, BytesView out, Index offset) -> bool
{
    return writer.write_at(out, offset) == out.size();
}

auto File::noex_size() const -> Result<Size>
{
    static constexpr auto INVALID_SIZE = static_cast<std::uintmax_t>(-1);

    std::error_code error;
    if (const auto size = fs::file_size(m_path, error); size != INVALID_SIZE)
        return size;
    return ErrorResult {Error::system_error(error.message())};
}

auto File::noex_open(const std::string &path, Mode mode, int permissions) -> Result<void>
{
    return system::noex_open(path, static_cast<int>(mode), permissions)
        .and_then([&path, mode, permissions, this](int fd) -> Result<void> {
            m_file = fd;
            m_path = path;
            m_mode = mode;
            m_permissions = permissions;
            return {};
        });
}

auto File::noex_close() -> Result<void>
{
    return system::noex_close(m_file)
        .and_then([this]() -> Result<void> {
            m_file = system::FAILURE;
            return {};
        });
}

auto File::noex_rename(const std::string &name) -> Result<void>
{
    CALICO_EXPECT_TRUE(m_path.has_parent_path());
    CALICO_EXPECT_FALSE(m_path.empty());
    std::error_code error;
    fs::rename(std::exchange(m_path, m_path.parent_path() / name), m_path, error);
    if (error)
        return ErrorResult {Error::system_error(error.message())};
    return {};
}

auto File::noex_remove() -> Result<void>
{
    return system::noex_unlink(m_path)
        .and_then([this]() -> Result<void> {
            m_path.clear();
            return {};
        });
}

auto noex_read_exact(IFileReader &reader, Bytes in) -> Result<void>
{
    return reader.noex_read(in)
        .and_then([in](Size read_size) {
            return read_size == in.size() ? Result<void> {} : ErrorResult {system::error(std::errc::io_error)};
        });
}

auto noex_read_exact_at(IFileReader &reader, Bytes in, Index offset) -> Result<void>
{
    return reader.noex_read_at(in, offset)
        .and_then([in](Size read_size) {
            return read_size == in.size() ? Result<void> {} : ErrorResult {system::error(std::errc::io_error)};
        });
}

auto noex_write_all(IFileWriter &writer, BytesView out) -> Result<void>
{
    return writer.noex_write(out)
        .and_then([out](Size write_size) {
            return write_size == out.size() ? Result<void> {} : ErrorResult {system::error(std::errc::io_error)};
        });
}

auto noex_write_all_at(IFileWriter &writer, BytesView out, Index offset) -> Result<void>
{
    return writer.noex_write_at(out, offset)
        .and_then([out](Size write_size) {
            return write_size == out.size() ? Result<void> {} : ErrorResult {system::error(std::errc::io_error)};
        });
}

} // calico
