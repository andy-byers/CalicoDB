#ifndef CUB_STORAGE_INTERFACE_H
#define CUB_STORAGE_INTERFACE_H

#include <fcntl.h>
#include <unistd.h>
#include "cub/bytes.h"
#include "cub/exception.h"

namespace cub {

enum class Seek: int {
    BEGIN   = SEEK_SET,
    CURRENT = SEEK_CUR,
    END     = SEEK_END,
};

enum class Mode: int {
    CREATE = O_CREAT,
    EXCLUSIVE = O_EXCL,
    SYNCHRONOUS = O_SYNC,
    TRUNCATE = O_TRUNC,

#if CUB_HAS_O_DIRECT
    DIRECT = O_DIRECT,
#else
    DIRECT = 0,
#endif
};

inline auto operator|(const Mode &lhs, const Mode &rhs)
{
    return static_cast<Mode>(static_cast<int>(lhs) | static_cast<int>(rhs));
}

class IReadOnlyFile {
public:
    virtual ~IReadOnlyFile() = default;
    [[nodiscard]] virtual auto size() const -> Size = 0;
    virtual auto use_direct_io() -> void = 0;
    virtual auto sync() -> void = 0;
    virtual auto seek(long, Seek) -> Index = 0;
    virtual auto read(Bytes) -> Size = 0;

    auto read_at(Bytes out, Index offset) -> Size
    {
        seek(static_cast<long>(offset), Seek::BEGIN);
        return read(out);
    }
};

class IWriteOnlyFile {
public:
    virtual ~IWriteOnlyFile() = default;
    [[nodiscard]] virtual auto size() const -> Size = 0;
    virtual auto use_direct_io() -> void = 0;
    virtual auto sync() -> void = 0;
    virtual auto resize(Size) -> void = 0;
    virtual auto seek(long, Seek) -> Index = 0;
    virtual auto write(BytesView) -> Size = 0;

    auto write_at(BytesView in, Index offset) -> Size
    {
        seek(static_cast<long>(offset), Seek::BEGIN);
        return write(in);
    }
};

class IReadWriteFile {
public:
    virtual ~IReadWriteFile() = default;
    [[nodiscard]] virtual auto size() const -> Size = 0;
    virtual auto use_direct_io() -> void = 0;
    virtual auto sync() -> void = 0;
    virtual auto resize(Size) -> void = 0;
    virtual auto seek(long, Seek) -> Index = 0;
    virtual auto read(Bytes) -> Size = 0;
    virtual auto write(BytesView) -> Size = 0;

    auto read_at(Bytes out, Index offset) -> Size
    {
        seek(static_cast<long>(offset), Seek::BEGIN);
        return read(out);
    }

    auto write_at(BytesView in, Index offset) -> Size
    {
        seek(static_cast<long>(offset), Seek::BEGIN);
        return write(in);
    }
};

class ILogFile {
public:
    virtual ~ILogFile() = default;
    [[nodiscard]] virtual auto size() const -> Size = 0;
    virtual auto use_direct_io() -> void = 0;
    virtual auto sync() -> void = 0;
    virtual auto resize(Size) -> void = 0;
    virtual auto write(BytesView) -> Size = 0;
};

template<class R> static auto read_exact(R &readable_store, Bytes out)
{
    if (readable_store.read(out) != out.size())
        throw IOError::partial_read();
}

template<class R> static auto read_exact_at(R &readable_store, Bytes out, Index offset)
{
    if (readable_store.read_at(out, offset) != out.size())
        throw IOError::partial_read();
}

template<class W> static auto write_exact(W &writable_store, BytesView in)
{
    if (writable_store.write(in) != in.size())
        throw IOError::partial_write();
}

template<class W> static auto write_exact_at(W &writable_store, BytesView in, Index offset)
{
    if (writable_store.write_at(in, offset) != in.size())
        throw IOError::partial_write();
}

} // cub

#endif // CUB_STORAGE_INTERFACE_H
