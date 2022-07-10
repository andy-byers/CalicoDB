#ifndef CALICO_STORAGE_DIRECTORY_H
#define CALICO_STORAGE_DIRECTORY_H

#include "calico/error.h"
#include "interface.h"
#include <filesystem>

namespace calico {

class IFile;

class Directory: public IDirectory {
public:
    ~Directory() override;
    static auto open(const std::string&) -> Result<std::unique_ptr<IDirectory>>;

    explicit Directory(const std::string&);
    [[nodiscard]] auto path() const -> std::string override;
    [[nodiscard]] auto name() const -> std::string override;
    [[nodiscard]] auto children() const -> std::vector<std::string> override;
    auto open_directory(const std::string&) -> std::unique_ptr<IDirectory> override;
    auto open_file(const std::string&, Mode, int) -> std::unique_ptr<IFile> override;
    auto remove() -> void override;
    auto sync() -> void override;

    auto noex_children() const -> Result<std::vector<std::string>> override;
    auto noex_open_directory(const std::string&) -> Result<std::unique_ptr<IDirectory>> override;
    auto noex_open_file(const std::string&, Mode, int) -> Result<std::unique_ptr<IFile>> override;
    auto noex_remove() -> Result<void> override;
    auto noex_sync() -> Result<void> override;

private:
    Directory() = default;

    std::filesystem::path m_path;
    int m_file {-1};
};

} // calico

#endif // CALICO_STORAGE_DIRECTORY_H
