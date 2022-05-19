#ifndef CUB_STORAGE_FILE_H
#define CUB_STORAGE_FILE_H

#include "common.h"
#include "interface.h"
#include "utils/slice.h"

namespace cub {

namespace {

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

} // <anonymous>

class ReadOnlyFile: public ReadOnlyStorage {
public:
    ReadOnlyFile(const std::string&, Mode, int);
    ~ReadOnlyFile() override;
    auto seek(long, Seek) -> Index override;
    auto read(MutBytes) -> Size override;

private:
    [[nodiscard]] auto fd() const -> int override
    {
        return m_resource.fd();
    }
    Resource m_resource;
};

class WriteOnlyFile: public WriteOnlyStorage {
public:
    WriteOnlyFile(const std::string&, Mode, int);
    ~WriteOnlyFile() override;
    auto resize(Size) -> void override;
    auto seek(long, Seek) -> Index override;
    auto write(RefBytes) -> Size override;

private:
    [[nodiscard]] auto fd() const -> int override
    {
        return m_resource.fd();
    }

    Resource m_resource;
};

class ReadWriteFile: public ReadWriteStorage {
public:
    ReadWriteFile(const std::string&, Mode, int);
    ~ReadWriteFile() override;
    auto resize(Size) -> void override;
    auto seek(long, Seek) -> Index override;
    auto read(MutBytes) -> Size override;
    auto write(RefBytes) -> Size override;

private:
    [[nodiscard]] auto fd() const -> int override
    {
        return m_resource.fd();
    }

    Resource m_resource;
};

class LogFile: public LogStorage {
public:
    LogFile(const std::string&, Mode, int);
    ~LogFile() override;
    auto resize(Size) -> void override;
    auto write(RefBytes) -> Size override;

private:
    [[nodiscard]] auto fd() const -> int override
    {
        return m_resource.fd();
    }

    Resource m_resource;
};

} // cub

#endif // CUB_STORAGE_FILE_H
