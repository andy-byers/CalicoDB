#include "calico/status.h"
#include "utils/utils.h"

namespace Calico {

static auto maybe_copy_data(const char *data) -> std::unique_ptr<char[]>
{
    // Status is OK, so there isn't anything to copy.
    if (data == nullptr) return nullptr;

    // Allocate memory for the copied message/status code.
    const auto total_size = std::char_traits<char>::length(data) + sizeof(char);
    auto copy = std::make_unique<char[]>(total_size);

    // Copy the status, std::make_unique<char[]>() will zero initialize, so we already have the null byte.
    std::memcpy(copy.get(), data, total_size - sizeof(char));
    return copy;
}

Status::Status(code code, const std::string_view &message)
{
    static constexpr Size EXTRA_SIZE {sizeof(code) + sizeof(char)};
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
    : m_data {maybe_copy_data(rhs.m_data.get())}
{}

Status::Status(Status &&rhs) noexcept
    : m_data {std::move(rhs.m_data)}
{}

auto Status::operator=(const Status &rhs) -> Status &
{
    if (this != &rhs)
        m_data = maybe_copy_data(rhs.m_data.get());
    return *this;
}

auto Status::operator=(Status &&rhs) noexcept -> Status &
{
    if (this != &rhs)
        m_data = std::move(rhs.m_data);
    return *this;
}

auto Status::ok() -> Status
{
    return Status {};
}

auto Status::not_found(const std::string_view &what) -> Status
{
    return {code::NOT_FOUND, what};
}

auto Status::invalid_argument(const std::string_view &what) -> Status
{
    return {code::INVALID_ARGUMENT, what};
}

auto Status::system_error(const std::string_view &what) -> Status
{
    return {code::SYSTEM_ERROR, what};
}

auto Status::logic_error(const std::string_view &what) -> Status
{
    return {code::LOGIC_ERROR, what};
}

auto Status::corruption(const std::string_view &what) -> Status
{
    return {code::CORRUPTION, what};
}

auto Status::is_invalid_argument() const -> bool
{
    return !is_ok() && code {m_data[0]} == code::INVALID_ARGUMENT;
}

auto Status::is_system_error() const -> bool
{
    return !is_ok() && code {m_data[0]} == code::SYSTEM_ERROR;
}

auto Status::is_logic_error() const -> bool
{
    return !is_ok() && code {m_data[0]} == code::LOGIC_ERROR;
}

auto Status::is_corruption() const -> bool
{
    return !is_ok() && code {m_data[0]} == code::CORRUPTION;
}

auto Status::is_not_found() const -> bool
{
    return !is_ok() && code {m_data[0]} == code::NOT_FOUND;
}

auto Status::is_ok() const -> bool
{
    return m_data == nullptr;
}

auto Status::what() const -> std::string_view
{
    return is_ok() ? "" : std::string_view {m_data.get() + sizeof(code)};
}

} // namespace Calico
