#ifndef CCO_STORAGE_FILE_H
#define CCO_STORAGE_FILE_H

#include "interface.h"
#include <filesystem>

namespace cco {

class File : public IFile {
public:
    ~File() override = default;
    [[nodiscard]] auto is_open() const -> bool override;
    [[nodiscard]] auto mode() const -> Mode override;
    [[nodiscard]] auto permissions() const -> int override;
    [[nodiscard]] auto file() const -> int override;
    [[nodiscard]] auto path() const -> std::string override;
    [[nodiscard]] auto name() const -> std::string override;
    [[nodiscard]] auto size() const -> Result<Size> override;
    [[nodiscard]] auto open(const std::string &, Mode, int) -> Result<void> override;
    [[nodiscard]] auto close() -> Result<void> override;
    [[nodiscard]] auto rename(const std::string &) -> Result<void> override;
    [[nodiscard]] auto resize(Size) -> Result<void> override;
    [[nodiscard]] auto remove() -> Result<void> override;
    [[nodiscard]] auto seek(long, Seek) -> Result<Index> override;
    [[nodiscard]] auto read(Bytes) -> Result<Size> override;
    [[nodiscard]] auto read(Bytes, Index) -> Result<Size> override;
    [[nodiscard]] auto write(BytesView) -> Result<Size> override;
    [[nodiscard]] auto write(BytesView, Index) -> Result<Size> override;
    [[nodiscard]] auto sync() -> Result<void> override;

private:
    std::filesystem::path m_path;
    Mode m_mode {};
    int m_permissions {};
    int m_file {-1};
};

auto read_exact(IFile &, Bytes) -> Result<void>;
auto read_exact_at(IFile &, Bytes, Index) -> Result<void>;
auto write_all(IFile &, BytesView) -> Result<void>;
auto write_all(IFile &, BytesView, Index) -> Result<void>;

} // namespace cco

#endif // CCO_STORAGE_FILE_H
