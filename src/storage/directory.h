#ifndef CALICO_STORAGE_DIRECTORY_H
#define CALICO_STORAGE_DIRECTORY_H

#include "interface.h"
#include <filesystem>

namespace calico {

class IFile;

class Directory: public IDirectory {
public:
    ~Directory() override;
    Directory(const std::string&);
    [[nodiscard]] auto path() const -> std::string override;
    [[nodiscard]] auto name() const -> std::string override;
    [[nodiscard]] auto children() const -> std::vector<std::string> override;
    auto open_directory(const std::string&) -> std::unique_ptr<IDirectory> override;
    auto open_file(const std::string&, Mode, int) -> std::unique_ptr<IFile> override;
    auto remove() -> void override;
    auto sync() -> void override;

private:
    std::filesystem::path m_path;
    int m_file {-1};
};

} // calico

#endif // CALICO_STORAGE_DIRECTORY_H
