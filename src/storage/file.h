#ifndef CCO_STORAGE_FILE_H
#define CCO_STORAGE_FILE_H

#include "interface.h"
#include <filesystem>

namespace cco {

class File : public IFile {
public:
    File(int file, Mode mode, std::string name)
        : m_name {std::move(name)},
          m_file {file},
          m_mode {mode}
    {}

    ~File() override = default;
    [[nodiscard]] auto is_open() const -> bool override;
    [[nodiscard]] auto mode() const -> Mode override;
    [[nodiscard]] auto file() const -> int override;
    [[nodiscard]] auto name() const -> std::string override;
    [[nodiscard]] auto size() const -> Result<Size> override;
    [[nodiscard]] auto close() -> Result<void> override;
    [[nodiscard]] auto resize(Size) -> Result<void> override;
    [[nodiscard]] auto seek(long, Seek) -> Result<Index> override;
    [[nodiscard]] auto read(Bytes) -> Result<Size> override;
    [[nodiscard]] auto read(Bytes, Index) -> Result<Size> override;
    [[nodiscard]] auto write(BytesView) -> Result<Size> override;
    [[nodiscard]] auto write(BytesView, Index) -> Result<Size> override;
    [[nodiscard]] auto sync() -> Result<void> override;

private:
    std::string m_name;
    int m_file {-1};
    Mode m_mode {};
};

auto read_exact(IFile &, Bytes) -> Result<void>;
auto read_exact_at(IFile &, Bytes, Index) -> Result<void>;
auto write_all(IFile &, BytesView) -> Result<void>;
auto write_all(IFile &, BytesView, Index) -> Result<void>;

} // namespace cco

#endif // CCO_STORAGE_FILE_H
