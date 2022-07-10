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

auto Directory::open(const std::string &path) -> Result<std::unique_ptr<IDirectory>>
{
    if (path.empty())
        return ErrorResult {Error::invalid_argument("cannot open directory: path cannot be empty")};

    std::error_code code;
    fs::create_directory(path, code);
    if (code)
        return ErrorResult {Error::system_error(code.message())};

    return system::noex_open(path, static_cast<int>(Mode::READ_ONLY), 0666)
        .and_then([path](int fd) {
            auto directory = std::unique_ptr<Directory>(new Directory);
            directory->m_file = fd;
            directory->m_path = path;
            return Result<std::unique_ptr<IDirectory>> {std::move(directory)};
        });
}

auto Directory::noex_remove() -> Result<void>
{
    // Note that the directory must be empty for this to succeed.
    std::error_code error;
    if (!fs::remove(m_path, error))
        return ErrorResult {Error::system_error(error.message())};
    return {};
}

auto Directory::noex_sync() -> Result<void>
{
    return system::noex_sync(m_file);
}

auto Directory::noex_children() const -> Result<std::vector<std::string>>
{
    std::error_code error;
    std::filesystem::directory_iterator itr {m_path, error};
    if (error)
        return ErrorResult {Error::system_error(error.message())};

    std::vector<std::string> out;
    for (auto const &entry : itr)
        out.emplace_back(entry.path());
    return out;
}

auto Directory::noex_open_directory(const std::string &name) -> Result<std::unique_ptr<IDirectory>>
{
    return std::make_unique<Directory>(m_path / name);
}

auto Directory::noex_open_file(const std::string &name, Mode mode, int permissions) -> Result<std::unique_ptr<IFile>>
{
    auto file = std::make_unique<File>();
    return file->noex_open(m_path / name, mode, permissions)
        .and_then([&file]() -> Result<std::unique_ptr<IFile>> {
            return std::move(file);
        });
}

} // calico