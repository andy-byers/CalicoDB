#include "calico/status.h"
#include "utils/utils.h"

namespace calico {

static auto copy_status(const char *status) -> std::unique_ptr<char[]>
{
    // Represents an OK status.
    if (!status)
        return nullptr;

    const auto size = std::strlen(status) + sizeof(char);
    auto copy = std::make_unique<char[]>(size);
    Bytes bytes {copy.get(), size};

    CALICO_EXPECT_EQ(status[size - 1], '\0');
    std::strcpy(bytes.data(), status);
    bytes[size - 1] = '\0';
    return copy;
}

Status::Status(Code code, const std::string &message)
{
    const auto size = message.size() + sizeof(Code) + sizeof(char);
    m_what = std::make_unique<char[]>(size);
    Bytes bytes {m_what.get(), size};

    bytes[0] = static_cast<char>(code);
    bytes.advance();

    mem_copy(bytes, stob(message));
    bytes.advance(message.size());

    // TODO: Does make_unique() write "new char[N]()", or "new char[N]"? The latter will not zero initialize the data and requires this next line.
    bytes[0] = '\0';
}

Status::Status(const Status &rhs)
    : m_what {copy_status(rhs.m_what.get())}
{}

Status::Status(Status &&rhs) noexcept
    : m_what {std::move(rhs.m_what)}
{}

auto Status::operator=(const Status &rhs) -> Status&
{
    if (this != &rhs)
        m_what = copy_status(rhs.m_what.get());
    return *this;
}

auto Status::operator=(Status &&rhs) noexcept -> Status&
{
    if (this != &rhs)
        m_what = std::move(rhs.m_what);
    return *this;
}

auto Status::code() const -> Code
{
    CALICO_EXPECT_FALSE(is_ok());
    return Code {m_what.get()[0]};
}

auto Status::ok() -> Status
{
    return Status {};
}

auto Status::not_found(const std::string &what) -> Status
{
    return {Code::NOT_FOUND, what};
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
    return !is_ok() && code() == Code::INVALID_ARGUMENT;
}

auto Status::is_system_error() const -> bool
{
    return !is_ok() && code() == Code::SYSTEM_ERROR;
}

auto Status::is_logic_error() const -> bool
{
    return !is_ok() && code() == Code::LOGIC_ERROR;
}

auto Status::is_corruption() const -> bool
{
    return !is_ok() && code() == Code::CORRUPTION;
}

auto Status::is_not_found() const -> bool
{
    return !is_ok() && code() == Code::NOT_FOUND;
}

auto Status::is_ok() const -> bool
{
    return m_what == nullptr;
}

auto Status::what() const -> std::string
{
    // We could just return a BytesView, but we usually end up needing to convert it to a string anyway.
    return is_ok() ? "" : std::string {m_what.get() + sizeof(Code)};
}

} // namespace calico
