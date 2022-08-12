#include "disk.h"
#include "system.h"
#include "utils/expect.h"
#include "utils/logging.h"

namespace cco {

namespace fs = std::filesystem;
static constexpr int PERMISSIONS {0644}; ///< -rw-r--r--

static auto read_file_at(int file, Bytes &out, Index offset)
{
    auto r = system::file_seek(file, static_cast<long>(offset), SEEK_SET);
    if (r.has_value())
        r = system::file_read(file, out);
    
    if (!r.has_value())
        return r.error();
    out.truncate(*r);
    return Status::ok();
}

static auto write_file(int file, BytesView in)
{
    const auto r = system::file_write(file, in);
    
    if (!r.has_value()) {
        return r.error();
    } else if (*r != in.size()) {
        ThreePartMessage message;
        message.set_primary("could not write to file");
        message.set_detail("incomplete write");
        message.set_hint("wrote {}/{} bytes", *r, in.size());
        return message.system_error();
    }
    return Status::ok();
}

RandomAccessFileReader::~RandomAccessFileReader()
{
    [[maybe_unused]] const auto s = system::file_close(m_file);
}

auto RandomAccessFileReader::read(Bytes &out, Index offset) -> Status
{
    return read_file_at(m_file, out, offset);
}

RandomAccessFileEditor::~RandomAccessFileEditor()
{
    [[maybe_unused]] const auto s = system::file_close(m_file);
}

auto RandomAccessFileEditor::read(Bytes &out, Index offset) -> Status
{
    return read_file_at(m_file, out, offset);
}

auto RandomAccessFileEditor::write(BytesView in, Index offset) -> Status
{
    auto r = system::file_seek(m_file, static_cast<long>(offset), SEEK_SET);
    if (r.has_value())
        return write_file(m_file, in);
    return r.error();
}

auto RandomAccessFileEditor::sync() -> Status
{
    return system::file_sync(m_file);
}

AppendFileWriter::~AppendFileWriter()
{
    [[maybe_unused]] const auto s = system::file_close(m_file);
}

auto AppendFileWriter::write(BytesView in) -> Status
{
    return write_file(m_file, in);
}

auto AppendFileWriter::sync() -> Status
{
    return system::file_sync(m_file);
}

auto DiskStorage::open(const std::string &path, Storage **out) -> Status
{
    const auto r = system::file_open(path, O_RDONLY, 0666);
    auto s = Status::ok();

    if (r.has_value()) {
        s = system::file_close(*r);
    } else if (r.error().is_not_found()) {
        std::error_code code;
        fs::create_directory(path, code);
        if (code) return Status::system_error(code.message());
    }
    *out = new DiskStorage {path};
    return s;
}

auto DiskStorage::resize_file(const std::string &name, Size size) -> Status
{
    std::error_code code;
    fs::resize_file(m_path / name, size, code);
    if (code) return Status::system_error(code.message());
    return Status::ok();
}

auto DiskStorage::rename_file(const std::string &old_name, const std::string &new_name) -> Status
{
    std::error_code code;
    fs::rename(m_path / old_name, m_path / new_name, code);
    if (code) return Status::system_error(code.message());
    return Status::ok();
}

auto DiskStorage::remove_file(const std::string &name) -> Status
{
    return system::file_remove(m_path / name);
}

auto DiskStorage::file_exists(const std::string &name) const -> Status
{
    return system::file_exists(m_path / name);
}

auto DiskStorage::file_size(const std::string &name, Size &out) const -> Status
{
    auto r = system::file_size(m_path / name);
    if (r.has_value()) {
        out = *r;
        return Status::ok();
    }
    return r.error();
}

auto DiskStorage::get_file_names(std::vector<std::string> &out) const -> Status
{
    std::error_code code;
    std::filesystem::directory_iterator itr {m_path, code};
    if (code)
        return Status::system_error(code.message());

    for (auto const &entry: itr)
        out.emplace_back(entry.path());
    return Status::ok();
}

auto DiskStorage::open_random_reader(const std::string &name, RandomAccessReader **out) -> Status
{
    const auto fd = system::file_open(m_path / name, O_RDONLY, PERMISSIONS);
    if (fd.has_value()) {
        *out = new RandomAccessFileReader {name, *fd};
        return Status::ok();
    }
    return fd.error();
}

auto DiskStorage::open_random_editor(const std::string &name, RandomAccessEditor **out) -> Status
{
    const auto fd = system::file_open(m_path / name, O_CREAT | O_RDWR, PERMISSIONS);
    if (fd.has_value()) {
        *out = new RandomAccessFileEditor {name, *fd};
        return Status::ok();
    }
    return fd.error();
}

auto DiskStorage::open_append_writer(const std::string &name, AppendWriter **out) -> Status
{
    const auto fd = system::file_open(m_path / name, O_CREAT | O_WRONLY | O_APPEND, PERMISSIONS);
    if (fd.has_value()) {
        *out = new AppendFileWriter {name, *fd};
        return Status::ok();
    }
    return fd.error();
}

auto DiskStorage::create_directory(const std::string &name) -> Status
{
    return system::dir_create(m_path / name);
}

auto DiskStorage::remove_directory(const std::string &name) -> Status
{
    return system::dir_remove(m_path / name);
}

} // namespace cco