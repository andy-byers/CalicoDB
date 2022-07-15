#ifndef CCO_STORAGE_INTERFACE_H
#define CCO_STORAGE_INTERFACE_H

#include <memory>
#include <string>
#include <vector>
#include <fcntl.h>
#include "calico/error.h"

namespace cco {

enum class Seek: int {
    BEGIN   = SEEK_SET,
    CURRENT = SEEK_CUR,
    END     = SEEK_END,
};

enum class Mode: int {
    READ_ONLY = O_RDONLY,
    WRITE_ONLY = O_WRONLY,
    READ_WRITE = O_RDWR,
    APPEND = O_RDWR,
    CREATE = O_CREAT,
    EXCLUSIVE = O_EXCL,
    SYNCHRONOUS = O_SYNC,
    TRUNCATE = O_TRUNC,

#if CCO_HAS_O_DIRECT
    DIRECT = O_DIRECT,
#else
    DIRECT = 0,
#endif
};

inline auto operator|(const Mode &lhs, const Mode &rhs)
{
    return static_cast<Mode>(static_cast<int>(lhs) | static_cast<int>(rhs));
}

class IFileReader {
public:
    virtual ~IFileReader() = default;
    [[nodiscard]] virtual auto seek(long, Seek) -> Result<Index> = 0;
    [[nodiscard]] virtual auto read(Bytes) -> Result<Size> = 0;
    [[nodiscard]] virtual auto read(Bytes, Index) -> Result<Size> = 0;
};

class IFileWriter {
public:
    virtual ~IFileWriter() = default;
    [[nodiscard]] virtual auto seek(long, Seek) -> Result<Index> = 0;
    [[nodiscard]] virtual auto write(BytesView) -> Result<Size> = 0;
    [[nodiscard]] virtual auto write(BytesView, Index) -> Result<Size> = 0;
    [[nodiscard]] virtual auto sync() -> Result<void> = 0;
    [[nodiscard]] virtual auto resize(Size) -> Result<void> = 0;
};

class IFile {
public:
    virtual ~IFile() = default;
    [[nodiscard]] virtual auto is_open() const -> bool = 0;
    [[nodiscard]] virtual auto mode() const -> Mode = 0;
    [[nodiscard]] virtual auto permissions() const -> int = 0;
    [[nodiscard]] virtual auto path() const -> std::string = 0;
    [[nodiscard]] virtual auto name() const -> std::string = 0;
    [[nodiscard]] virtual auto file() const -> int = 0;
    [[nodiscard]] virtual auto size() const -> Result<Size> = 0;
    [[nodiscard]] virtual auto open_reader() -> std::unique_ptr<IFileReader> = 0;
    [[nodiscard]] virtual auto open_writer() -> std::unique_ptr<IFileWriter> = 0;
    [[nodiscard]] virtual auto open(const std::string&, Mode, int) -> Result<void> = 0;
    [[nodiscard]] virtual auto close() -> Result<void> = 0;
    [[nodiscard]] virtual auto rename(const std::string&) -> Result<void> = 0;
    [[nodiscard]] virtual auto remove() -> Result<void> = 0;
};

class IDirectory {
public:
    virtual ~IDirectory() = default;
    [[nodiscard]] virtual auto path() const -> std::string = 0;
    [[nodiscard]] virtual auto name() const -> std::string = 0;
    [[nodiscard]] virtual auto exists(const std::string&) const -> Result<bool> = 0;
    [[nodiscard]] virtual auto children() const -> Result<std::vector<std::string>> = 0;
    [[nodiscard]] virtual auto open_directory(const std::string&) -> Result<std::unique_ptr<IDirectory>> = 0;
    [[nodiscard]] virtual auto open_file(const std::string&, Mode, int) -> Result<std::unique_ptr<IFile>> = 0;
    [[nodiscard]] virtual auto remove() -> Result<void> = 0;
    [[nodiscard]] virtual auto sync() -> Result<void> = 0;
    [[nodiscard]] virtual auto close() -> Result<void> = 0;
};

} // calico

#endif // CCO_STORAGE_INTERFACE_H
