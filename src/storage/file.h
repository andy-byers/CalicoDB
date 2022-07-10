#ifndef CALICO_STORAGE_FILE_H
#define CALICO_STORAGE_FILE_H

#include "interface.h"
#include <filesystem>

namespace calico {

class File: public IFile {
public:
    ~File() override = default;
    [[nodiscard]] auto is_open() const -> bool override;
    [[nodiscard]] auto is_readable() const -> bool override;
    [[nodiscard]] auto is_writable() const -> bool override;
    [[nodiscard]] auto is_append() const -> bool override;
    [[nodiscard]] auto permissions() const -> int override;
    [[nodiscard]] auto file() const -> int override;
    [[nodiscard]] auto path() const -> std::string override;
    [[nodiscard]] auto name() const -> std::string override;
    [[nodiscard]] auto size() const -> Size override;
    [[nodiscard]] auto open_reader() -> std::unique_ptr<IFileReader> override;
    [[nodiscard]] auto open_writer() -> std::unique_ptr<IFileWriter> override;
    auto open(const std::string&, Mode, int) -> void override;
    auto close() -> void override;
    auto rename(const std::string&) -> void override;
    auto remove() -> void override;

    [[nodiscard]] auto noex_size() const -> Result<Size> override;
    auto noex_open(const std::string&, Mode, int) -> Result<void> override;
    auto noex_close() -> Result<void> override;
    auto noex_rename(const std::string&) -> Result<void> override;
    auto noex_remove() -> Result<void> override;

private:
    std::filesystem::path m_path;
    Mode m_mode {};
    int m_permissions {};
    int m_file {-1};
};

auto read_exact(IFileReader&, Bytes) -> bool;
auto read_exact_at(IFileReader&, Bytes, Index) -> bool;
auto write_all(IFileWriter&, BytesView) -> bool;
auto write_all_at(IFileWriter&, BytesView, Index) -> bool;

auto noex_read_exact(IFileReader&, Bytes) -> Result<void>;
auto noex_read_exact_at(IFileReader&, Bytes, Index) -> Result<void>;
auto noex_write_all(IFileWriter&, BytesView) -> Result<void>;
auto noex_write_all_at(IFileWriter&, BytesView, Index) -> Result<void>;

} // calico

#endif // CALICO_STORAGE_FILE_H
