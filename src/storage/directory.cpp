#include "directory.h"
#include "file.h"
#include "system.h"

namespace calico {

namespace fs = std::filesystem;

Directory::~Directory()
{
    system::close(m_file)
        .or_else([](const Error &error) -> Result<void> {
            // TODO: Log this error.
            return ErrorResult {error};
        });
}

auto Directory::path() const -> std::string
{
    return m_path;
}

auto Directory::name() const -> std::string
{
    return m_path.filename();
}

auto Directory::open(const std::string &path) -> Result<std::unique_ptr<IDirectory>>
{
    if (path.empty())
        return ErrorResult {Error::invalid_argument("cannot open directory: path cannot be empty")};

    std::error_code code;
    fs::create_directory(path, code);
    if (code)
        return ErrorResult {Error::system_error(code.message())};

    return system::open(path, static_cast<int>(Mode::READ_ONLY), 0666)
        .and_then([path](int fd) {
            auto directory = std::unique_ptr<Directory>(new Directory);
            directory->m_file = fd;
            directory->m_path = path;
            return Result<std::unique_ptr<IDirectory>> {std::move(directory)};
        });
}

auto Directory::remove() -> Result<void>
{
    // Note that the directory must be empty for this to succeed.
    std::error_code error;
    if (!fs::remove(m_path, error))
        return ErrorResult {Error::system_error(error.message())};
    return {};
}

auto Directory::sync() -> Result<void>
{
    return system::sync(m_file);
}

auto Directory::children() const -> Result<std::vector<std::string>>
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

auto Directory::open_directory(const std::string &name) -> Result<std::unique_ptr<IDirectory>>
{
    return open(m_path / name);
}

auto Directory::open_file(const std::string &name, Mode mode, int permissions) -> Result<std::unique_ptr<IFile>>
{
    auto file = std::make_unique<File>();
    return file->open(m_path / name, mode, permissions)
        .and_then([&file]() -> Result<std::unique_ptr<IFile>> {
            return std::move(file);
        });
}

} // calico