
#include "calico/error.h"
#include "utils/expect.h"

namespace cco {

Error::Error(Code code, const std::string &message)
    : m_what {static_cast<char>(code) + message}
{
    CCO_EXPECT_NE(m_what[0], '\x00');
}

auto Error::code() const -> Code
{
    return Code {m_what[0]};
}

auto Error::invalid_argument(const std::string &what) -> Error
{
    return {Code::INVALID_ARGUMENT, what};
}

auto Error::system_error(const std::string &what) -> Error
{
    return {Code::SYSTEM_ERROR, what};
}

auto Error::logic_error(const std::string &what) -> Error
{
    return {Code::LOGIC_ERROR, what};
}

auto Error::corruption(const std::string &what) -> Error
{
    return {Code::CORRUPTION, what};
}

auto Error::not_found(const std::string &what) -> Error
{
    return {Code::NOT_FOUND, what};
}

auto Error::is_invalid_argument() const -> bool
{
    return code() == Code::INVALID_ARGUMENT;
}

auto Error::is_system_error() const -> bool
{
    return code() == Code::SYSTEM_ERROR;
}

auto Error::is_logic_error() const -> bool
{
    return code() == Code::LOGIC_ERROR;
}

auto Error::is_corruption() const -> bool
{
    return code() == Code::CORRUPTION;
}

auto Error::is_not_found() const -> bool
{
    return code() == Code::NOT_FOUND;
}

auto Error::what() const -> BytesView
{
    return stob(m_what).advance();
}

} // calico

