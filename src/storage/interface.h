#ifndef CUB_INTERFACE_H
#define CUB_INTERFACE_H

#include <fcntl.h>
#include <unistd.h>
#include "common.h"
#include "exception.h"
#include "system.h"
#include "utils/slice.h"

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
    DIRECT = O_DIRECT,
};

inline auto operator|(const Mode &lhs, const Mode &rhs)
{
    return static_cast<Mode>(static_cast<int>(lhs) | static_cast<int>(rhs));
}

class ReadOnlyStorage {
public:
    virtual ~ReadOnlyStorage() = default;
    virtual auto seek(long, Seek) -> Index = 0;
    virtual auto read(MutBytes) -> Size = 0;

    auto read_at(MutBytes out, Index offset) -> Size
    {
        seek(static_cast<long>(offset), Seek::BEGIN);
        return read(out);
    }

private:
    friend class Fs;
    [[nodiscard]] virtual auto fd() const -> int
    {
        return -1;
    }
};

class WriteOnlyStorage {
public:
    virtual ~WriteOnlyStorage() = default;
    virtual auto resize(Size) -> void = 0;
    virtual auto seek(long, Seek) -> Index = 0;
    virtual auto write(RefBytes) -> Size = 0;

    auto write_at(RefBytes in, Index offset) -> Size
    {
        seek(static_cast<long>(offset), Seek::BEGIN);
        return write(in);
    }

private:
    friend class Fs;
    [[nodiscard]] virtual auto fd() const -> int
    {
        return -1;
    }
};

class ReadWriteStorage {
public:
    friend class Fs;
    virtual ~ReadWriteStorage() = default;
    virtual auto resize(Size) -> void = 0;
    virtual auto seek(long, Seek) -> Index = 0;
    virtual auto read(MutBytes) -> Size = 0;
    virtual auto write(RefBytes) -> Size = 0;

    auto read_at(MutBytes out, Index offset) -> Size
    {
        seek(static_cast<long>(offset), Seek::BEGIN);
        return read(out);
    }

    auto write_at(RefBytes in, Index offset) -> Size
    {
        seek(static_cast<long>(offset), Seek::BEGIN);
        return write(in);
    }

private:
    friend class Fs;
    [[nodiscard]] virtual auto fd() const -> int
    {
        return -1;
    }
};

class LogStorage {
public:
    friend class Fs;
    virtual ~LogStorage() = default;
    virtual auto resize(Size) -> void = 0;
    virtual auto write(RefBytes) -> Size = 0;

private:
    friend class Fs;
    [[nodiscard]] virtual auto fd() const -> int
    {
        return -1;
    }
};

class Fs {
public:
    template<class S> static auto use_direct_io(const S &store)
    {
        system::use_direct_io(store.fd());
    }

    template<class S> static auto sync(const S &store)
    {
        system::sync(store.fd());
    }

    template<class S> static auto size(const S &store)
    {
        return system::size(store.fd());
    }

    template<class R> static auto read_exact(R &readable_store, MutBytes out)
    {
        if (readable_store.read(out) != out.size())
            throw IOError::partial_read();
    }

    template<class R> static auto read_exact_at(R &readable_store, MutBytes out, Index offset)
    {
        if (readable_store.read_at(out, offset) != out.size())
            throw IOError::partial_read();
    }

    template<class W> static auto write_exact(W &writable_store, RefBytes in)
    {
        if (writable_store.write(in) != in.size())
            throw IOError::partial_write();
    }

    template<class W> static auto write_exact_at(W &writable_store, RefBytes in, Index offset)
    {
        if (writable_store.write_at(in, offset) != in.size())
            throw IOError::partial_write();
    }
};

} // cub

#endif //CUB_INTERFACE_H
