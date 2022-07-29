#include "calico/status.h"

namespace cco {

Status::Status(Code code)
    : m_what(1, static_cast<char>(code))
{}

Status::Status(Code code, const std::string &message)
    : m_what {static_cast<char>(code) + message}
{}

auto Status::code() const -> Code
{
    return Code {m_what[0]};
}

auto Status::ok() -> Status
{
    return Status {Code::OK};
}

auto Status::not_found() -> Status
{
    return Status {Code::NOT_FOUND};
}

auto Status::invalid_argument(const std::string &what) -> Status
{
    return {Code::INVALID_ARGUMENT, what};
}

auto Status::system_error(const std::string &what) -> Status
{
    return {Code::SYSTEM_ERROR, what};
}

auto Status::logic_error(const std::string &what) -> Status
{
    return {Code::LOGIC_ERROR, what};
}

auto Status::corruption(const std::string &what) -> Status
{
    return {Code::CORRUPTION, what};
}

auto Status::is_invalid_argument() const -> bool
{
    return code() == Code::INVALID_ARGUMENT;
}

auto Status::is_system_error() const -> bool
{
    return code() == Code::SYSTEM_ERROR;
}

auto Status::is_logic_error() const -> bool
{
    return code() == Code::LOGIC_ERROR;
}

auto Status::is_corruption() const -> bool
{
    return code() == Code::CORRUPTION;
}

auto Status::is_not_found() const -> bool
{
    return code() == Code::NOT_FOUND;
}

auto Status::is_ok() const -> bool
{
    return code() == Code::OK;
}

auto Status::what() const -> std::string
{
    // We could just return a BytesView, but we usually end up needing to convert it to a string anyway.
    return btos(stob(m_what).advance());
}

} // namespace cco
