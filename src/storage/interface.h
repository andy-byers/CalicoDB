#ifndef CALICO_STORAGE_INTERFACE_H
#define CALICO_STORAGE_INTERFACE_H

#include "calico/bytes.h"
#include <memory>
#include <string>
#include <vector>
#include <fcntl.h>

namespace calico {

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

#if CALICO_HAS_O_DIRECT
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
    virtual auto seek(long, Seek) -> void = 0;
    virtual auto read(Bytes) -> Size = 0;
    virtual auto read_at(Bytes, Index) -> Size = 0;
};

class IFileWriter {
public:
    virtual ~IFileWriter() = default;
    virtual auto seek(long, Seek) -> void = 0;
    virtual auto write(BytesView) -> Size = 0;
    virtual auto write_at(BytesView, Index) -> Size = 0;
    virtual auto sync() -> void = 0;
    virtual auto resize(Size) -> void = 0;
};

class IFile {
public:
    virtual ~IFile() = default;
    [[nodiscard]] virtual auto is_open() const -> bool = 0;
    [[nodiscard]] virtual auto is_readable() const -> bool = 0;
    [[nodiscard]] virtual auto is_writable() const -> bool = 0;
    [[nodiscard]] virtual auto is_append() const -> bool = 0;
    [[nodiscard]] virtual auto permissions() const -> int = 0;
    [[nodiscard]] virtual auto path() const -> std::string = 0;
    [[nodiscard]] virtual auto name() const -> std::string = 0;
    [[nodiscard]] virtual auto size() const -> Size = 0;
    [[nodiscard]] virtual auto file() const -> int = 0;
    [[nodiscard]] virtual auto open_reader() -> std::unique_ptr<IFileReader> = 0;
    [[nodiscard]] virtual auto open_writer() -> std::unique_ptr<IFileWriter> = 0;
    virtual auto open(const std::string&, Mode, int) -> void = 0;
    virtual auto close() -> void = 0;
    virtual auto rename(const std::string&) -> void = 0;
    virtual auto remove() -> void = 0;
};

class IDirectory {
public:
    virtual ~IDirectory() = default;
    [[nodiscard]] virtual auto path() const -> std::string = 0;
    [[nodiscard]] virtual auto name() const -> std::string = 0;
    [[nodiscard]] virtual auto children() const -> std::vector<std::string> = 0;
    virtual auto open_file(const std::string&, Mode, int) -> std::unique_ptr<IFile> = 0;
    virtual auto open_directory(const std::string&) -> std::unique_ptr<IDirectory> = 0;
    virtual auto remove() -> void = 0;
    virtual auto sync() -> void = 0;
};

} // calico

#endif // CALICO_STORAGE_INTERFACE_H
