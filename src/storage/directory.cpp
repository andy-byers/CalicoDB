#include "directory.h"
#include "file.h"
#include "system.h"

namespace calico {

namespace fs = std::filesystem;

Directory::Directory(const std::string &path)
    : m_path {path}
{
    if (m_path.empty()) {
        throw std::invalid_argument {"Cannot open directory: Path cannot be empty"};
    }
    fs::create_directory(m_path);
    m_file = system::open(path, static_cast<int>(Mode::READ_ONLY), 0666);
}

Directory::~Directory()
{
    try {
        system::sync(m_file);
        system::close(m_file);
    } catch (...) {
        // TODO: Logger for directory to log this error? Or separate Directory::close()/Directory::sync() methods? FD is
        //       only needed for sync'ing the directory.
    }
}

auto Directory::path() const -> std::string
{
    return m_path;
}

auto Directory::name() const -> std::string
{
    return m_path.filename();
}

auto Directory::children() const -> std::vector<std::string>
{
    std::vector<std::string> out;
    for (auto const &entry : std::filesystem::directory_iterator {m_path})
        out.emplace_back(entry.path());
    return out;
}

auto Directory::open_directory(const std::string &name) -> std::unique_ptr<IDirectory>
{
    return std::make_unique<Directory>(m_path / name);
}

auto Directory::open_file(const std::string &name, Mode mode, int permissions) -> std::unique_ptr<IFile>
{
    auto file = std::make_unique<File>();
    file->open(m_path / name, mode, permissions);
    return file;
}

auto Directory::remove() -> void
{
    // Note that the directory must be empty for this to succeed.
    if (!fs::remove(m_path))
        throw std::logic_error {"Cannot remove directory; Directory must be empty"};
}

auto Directory::sync() -> void
{
    system::sync(m_file);
}

} // calico