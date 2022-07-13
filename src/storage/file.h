#ifndef CALICO_STORAGE_FILE_H
#define CALICO_STORAGE_FILE_H

#include "interface.h"
#include <filesystem>

namespace calico {

class File: public IFile {
public:
    ~File() override = default;
    [[nodiscard]] auto is_open() const -> bool override;
    [[nodiscard]] auto mode() const -> Mode override;
    [[nodiscard]] auto permissions() const -> int override;
    [[nodiscard]] auto file() const -> int override;
    [[nodiscard]] auto path() const -> std::string override;
    [[nodiscard]] auto name() const -> std::string override;
    [[nodiscard]] auto open_reader() -> std::unique_ptr<IFileReader> override;
    [[nodiscard]] auto open_writer() -> std::unique_ptr<IFileWriter> override;
    [[nodiscard]] auto size() const -> Result<Size> override;
    [[nodiscard]] auto open(const std::string&, Mode, int) -> Result<void> override;
    [[nodiscard]] auto close() -> Result<void> override;
    [[nodiscard]] auto rename(const std::string&) -> Result<void> override;
    [[nodiscard]] auto remove() -> Result<void> override;

private:
    std::filesystem::path m_path;
    Mode m_mode {};
    int m_permissions {};
    int m_file {-1};
};

auto read_exact(IFileReader&, Bytes) -> Result<void>;
auto read_exact_at(IFileReader&, Bytes, Index) -> Result<void>;
auto write_all(IFileWriter&, BytesView) -> Result<void>;
auto write_all(IFileWriter&, BytesView, Index) -> Result<void>;

} // calico

#endif // CALICO_STORAGE_FILE_H
