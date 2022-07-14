#ifndef CCO_STORAGE_DIRECTORY_H
#define CCO_STORAGE_DIRECTORY_H

#include "calico/error.h"
#include "interface.h"
#include <filesystem>

namespace cco {

class IFile;

class Directory: public IDirectory {
public:
    ~Directory() override;
    [[nodiscard]] static auto open(const std::string&) -> Result<std::unique_ptr<IDirectory>>;
    [[nodiscard]] auto path() const -> std::string override;
    [[nodiscard]] auto name() const -> std::string override;
    [[nodiscard]] auto exists(const std::string&) const -> Result<bool> override;
    [[nodiscard]] auto children() const -> Result<std::vector<std::string>> override;
    [[nodiscard]] auto open_directory(const std::string&) -> Result<std::unique_ptr<IDirectory>> override;
    [[nodiscard]] auto open_file(const std::string&, Mode, int) -> Result<std::unique_ptr<IFile>> override;
    [[nodiscard]] auto remove() -> Result<void> override;
    [[nodiscard]] auto sync() -> Result<void> override;

private:
    Directory() = default;

    std::filesystem::path m_path;
    int m_file {-1};
};

} // calico

#endif // CCO_STORAGE_DIRECTORY_H
