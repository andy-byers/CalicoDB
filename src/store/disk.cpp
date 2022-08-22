#include "disk.h"
#include "system.h"
#include "utils/expect.h"
#include "utils/logging.h"

namespace calico {

namespace fs = std::filesystem;
static constexpr int FILE_PERMISSIONS {0644}; ///< -rw-r--r--

static auto read_file_at(int file, Bytes &out, Size offset)
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

RandomFileReader::~RandomFileReader()
{
    [[maybe_unused]] const auto s = system::file_close(m_file);
}

auto RandomFileReader::read(Bytes &out, Size offset) -> Status
{
    return read_file_at(m_file, out, offset);
}

RandomFileEditor::~RandomFileEditor()
{
    [[maybe_unused]] const auto s = system::file_close(m_file);
}

auto RandomFileEditor::read(Bytes &out, Size offset) -> Status
{
    return read_file_at(m_file, out, offset);
}

auto RandomFileEditor::write(BytesView in, Size offset) -> Status
{
    auto r = system::file_seek(m_file, static_cast<long>(offset), SEEK_SET);
    if (r.has_value())
        return write_file(m_file, in);
    return r.error();
}

auto RandomFileEditor::sync() -> Status
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

auto DiskStorage::resize_file(const std::string &path, Size size) -> Status
{
    return system::file_resize(path, size);
}

auto DiskStorage::rename_file(const std::string &old_path, const std::string &new_path) -> Status
{
    std::error_code code;
    fs::rename(old_path, new_path, code);
    if (code) return Status::system_error(code.message());
    return Status::ok();
}

auto DiskStorage::remove_file(const std::string &path) -> Status
{
    return system::file_remove(path);
}

auto DiskStorage::file_exists(const std::string &path) const -> Status
{
    return system::file_exists(path);
}

auto DiskStorage::file_size(const std::string &path, Size &out) const -> Status
{
    auto r = system::file_size(path);
    if (r.has_value()) {
        out = *r;
        return Status::ok();
    }
    return r.error();
}

auto DiskStorage::get_children(const std::string &path, std::vector<std::string> &out) const -> Status
{
    std::error_code error;
    std::filesystem::directory_iterator itr {path, error};
    if (error) return Status::system_error(error.message());

    for (auto const &entry: itr)
        out.emplace_back(entry.path());
    return Status::ok();
}

auto DiskStorage::open_random_reader(const std::string &path, RandomReader **out) -> Status
{
    const auto fd = system::file_open(path, O_RDONLY, FILE_PERMISSIONS);
    if (fd.has_value()) {
        *out = new RandomFileReader {path, *fd};
        return Status::ok();
    }
    return fd.error();
}

auto DiskStorage::open_random_editor(const std::string &path, RandomEditor **out) -> Status
{
    const auto fd = system::file_open(path, O_CREAT | O_RDWR, FILE_PERMISSIONS);
    if (fd.has_value()) {
        *out = new RandomFileEditor {path, *fd};
        return Status::ok();
    }
    return fd.error();
}

auto DiskStorage::open_append_writer(const std::string &path, AppendWriter **out) -> Status
{
    const auto fd = system::file_open(path, O_CREAT | O_WRONLY | O_APPEND, FILE_PERMISSIONS);
    if (fd.has_value()) {
        *out = new AppendFileWriter {path, *fd};
        return Status::ok();
    }
    return fd.error();
}

auto DiskStorage::create_directory(const std::string &path) -> Status
{
    return system::dir_create(path, 0755);
}

auto DiskStorage::remove_directory(const std::string &path) -> Status
{
    return system::dir_remove(path);
}

} // namespace cco