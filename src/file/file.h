#ifndef CUB_FILE_FILE_H
#define CUB_FILE_FILE_H

#include "interface.h"

namespace cub {

class Resource {
public:
    Resource(const std::string&, int, Mode, int);
    virtual ~Resource();

    [[nodiscard]] auto fd() const -> int
    {
        return m_fd;
    }

private:
    int m_fd{};
};

class ReadOnlyFile: public IReadOnlyFile {
public:
    ReadOnlyFile(const std::string&, Mode, int);
    ~ReadOnlyFile() override;
    [[nodiscard]] auto size() const -> Size override;
    auto use_direct_io() -> void override;
    auto sync() -> void override;
    auto seek(long, Seek) -> Index override;
    auto read(Bytes) -> Size override;

private:
    Resource m_resource;
};

class WriteOnlyFile: public IWriteOnlyFile {
public:
    WriteOnlyFile(const std::string&, Mode, int);
    ~WriteOnlyFile() override;
    [[nodiscard]] auto size() const -> Size override;
    auto use_direct_io() -> void override;
    auto sync() -> void override;
    auto resize(Size) -> void override;
    auto seek(long, Seek) -> Index override;
    auto write(BytesView) -> Size override;

private:
    Resource m_resource;
};

class ReadWriteFile: public IReadWriteFile {
public:
    ReadWriteFile(const std::string&, Mode, int);
    ~ReadWriteFile() override;
    [[nodiscard]] auto size() const -> Size override;
    auto use_direct_io() -> void override;
    auto sync() -> void override;
    auto resize(Size) -> void override;
    auto seek(long, Seek) -> Index override;
    auto read(Bytes) -> Size override;
    auto write(BytesView) -> Size override;

private:
    Resource m_resource;
};

class LogFile: public ILogFile {
public:
    LogFile(const std::string&, Mode, int);
    ~LogFile() override;
    [[nodiscard]] auto size() const -> Size override;
    auto use_direct_io() -> void override;
    auto sync() -> void override;
    auto resize(Size) -> void override;
    auto write(BytesView) -> Size override;

private:
    Resource m_resource;
};

} // cub

#endif // CUB_FILE_FILE_H
