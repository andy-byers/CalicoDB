#ifndef CUB_EXCEPTION_H
#define CUB_EXCEPTION_H

#include <cerrno>
#include <cstring>
#include <exception>
#include <system_error>
#include <utility>
#include "common.h"

namespace cub {

class Exception: public std::exception {
public:
    // TODO: Constructing a std::string can throw. We probably don't want that to happen in this constructor...
    //       Could just store a const char* for `what` instead, but it makes it difficult to assemble nice error messages.
    //       Maybe look into fmt library?
    explicit Exception(std::string what)
        : m_what {std::move(what)} {}

    ~Exception() override = default;

    [[nodiscard]] auto what() const noexcept -> const char* override
    {
        return m_what.c_str();
    }

protected:
    std::string m_what;
};

class SystemError: public Exception {
public:
    SystemError(const std::string &name, int code)
        : Exception(name + ": " + strerror(code))
          , m_code {code, std::system_category()} {}

    explicit SystemError(const std::string &name)
        : Exception(name + ": " + strerror(errno))
          , m_code {std::exchange(errno, 0), std::system_category()} {}

    ~SystemError() override = default;

    [[nodiscard]] auto code() const noexcept -> std::error_code
    {
        return m_code;
    }

    [[nodiscard]] auto name() const noexcept -> const char*
    {
        return m_name.c_str();
    }

private:
    std::string m_name;
    std::error_code m_code;
};

class IOError: public SystemError {
public:
    static auto partial_read() -> IOError
    {
        return IOError {SystemError {"read (partial)", std::make_error_code(std::errc::io_error).value()}}; // TODO: Better way?
    }

    static auto partial_write() -> IOError
    {
        return IOError {SystemError {"write (partial)", std::make_error_code(std::errc::io_error).value()}};
    }

    explicit IOError(const SystemError &error)
        : SystemError {error} {}

    ~IOError() override = default;
};

class CorruptionError: public Exception {
public:
    explicit CorruptionError(const std::string &what)
        : Exception {what} {}

    ~CorruptionError() override = default;
};

class InvalidArgumentError: public Exception {
public:
    explicit InvalidArgumentError(const std::string &what)
        : Exception {what} {}

    ~InvalidArgumentError() override = default;
};

class InvalidOperationError: public Exception {
public:
    explicit InvalidOperationError(const std::string &what)
        : Exception {what} {}

    ~InvalidOperationError() override = default;
};

} // cub

#endif // CUB_EXCEPTION_H
