#ifndef CUB_EXCEPTION_H
#define CUB_EXCEPTION_H

#include <cerrno>
#include <cstring>
#include <exception>
#include <system_error>
#include <utility>
#include "common.h"

namespace cub {

class IOError: public std::runtime_error {
public:
    IOError(std::error_code code, const char *what)
        : std::runtime_error {what}
        , m_code {code} {}

    explicit IOError(const char *what)
        : std::runtime_error {what}
          , m_code {std::make_error_code(std::errc::io_error)} {}

    explicit IOError(const std::system_error &error)
        : std::runtime_error {error.what()}
        , m_code {error.code()} {}

    ~IOError() override = default;

    [[nodiscard]] auto code() const noexcept -> std::error_code
    {
        return m_code;
    }

private:
    std::error_code m_code {};
};

class CorruptionError: public std::runtime_error {
public:
    explicit CorruptionError(const std::string &what)
        : std::runtime_error {what} {}

    ~CorruptionError() override = default;
};

} // cub

#endif // CUB_EXCEPTION_H
