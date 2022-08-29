#include "calico/status.h"
#include "utils/utils.h"

namespace calico {

static auto copy_status(const char *status) -> std::unique_ptr<char[]>
{
    // Status is OK, so there isn't anything to copy.
    if (!status) return nullptr;

    // Otherwise, we have to copy a code and a message.
    const auto size = std::strlen(status) + sizeof(char);
    auto copy = std::make_unique<char[]>(size);
    Bytes bytes {copy.get(), size};

    CALICO_EXPECT_EQ(status[size - 1], '\0');
    std::strcpy(bytes.data(), status);
    bytes[size - 1] = '\0';
    return copy;
}

Status::Status(Code code, const std::string_view &message)
{
    static constexpr Size EXTRA_SIZE {sizeof(Code) + sizeof(char)};
    const auto size = message.size() + EXTRA_SIZE;

    m_data = std::make_unique<char[]>(size);
    Bytes bytes {m_data.get(), size};

    // The first byte holds the status type.
    bytes[0] = static_cast<char>(code);
    bytes.advance();

    // The rest holds the message, plus a '\0'. std::make_unique<char[]>() performs value initialization, so the byte is already
    // zeroed out. See https://en.cppreference.com/w/cpp/memory/unique_ptr/make_unique, overload (2).
    mem_copy(bytes, stob(message));
}

Status::Status(const Status &rhs)
    : m_data {copy_status(rhs.m_data.get())}
{}

Status::Status(Status &&rhs) noexcept
    : m_data {std::move(rhs.m_data)}
{}

auto Status::operator=(const Status &rhs) -> Status&
{
    if (this != &rhs)
        m_data = copy_status(rhs.m_data.get());
    return *this;
}

auto Status::operator=(Status &&rhs) noexcept -> Status&
{
    if (this != &rhs)
        m_data = std::move(rhs.m_data);
    return *this;
}

auto Status::code() const -> Code
{
    CALICO_EXPECT_FALSE(is_ok());
    return Code {m_data.get()[0]};
}

auto Status::ok() -> Status
{
    return Status {};
}

auto Status::not_found(const std::string_view &what) -> Status
{
    return {Code::NOT_FOUND, what};
}

auto Status::invalid_argument(const std::string_view &what) -> Status
{
    return {Code::INVALID_ARGUMENT, what};
}

auto Status::system_error(const std::string_view &what) -> Status
{
    return {Code::SYSTEM_ERROR, what};
}

auto Status::logic_error(const std::string_view &what) -> Status
{
    return {Code::LOGIC_ERROR, what};
}

auto Status::corruption(const std::string_view &what) -> Status
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
    return m_data == nullptr;
}

auto Status::what() const -> std::string_view
{
    return is_ok() ? "" : std::string_view {m_data.get() + sizeof(Code)};
}

} // namespace calico
