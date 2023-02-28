#include "calicodb/status.h"
#include "calicodb/slice.h"

namespace calicodb
{

enum Code : char {
    C_InvalidArgument = 1,
    C_SystemError = 2,
    C_LogicError = 3,
    C_Corruption = 4,
    C_NotFound = 5,
};

static auto maybe_copy_data(const char *data) -> std::unique_ptr<char[]>
{
    // Status is OK, so there isn't anything to copy.
    if (data == nullptr) {
        return nullptr;
    }
    // Allocate memory for the copied message/status code.
    const auto total_size = std::char_traits<char>::length(data) + sizeof(char);
    auto copy = std::make_unique<char[]>(total_size);

    // Copy the status, std::make_unique<char[]>() will zero initialize, so we already have the null byte.
    std::memcpy(copy.get(), data, total_size - sizeof(char));
    return copy;
}

Status::Status(char code, const Slice &what)
    : m_data {std::make_unique<char[]>(what.size() + 2 * sizeof(char))}
{
    auto *ptr = m_data.get();

    // The first byte holds the status type.
    *ptr++ = code;

    // The rest holds the message, plus a '\0'. std::make_unique<char[]>() performs value initialization, so the byte is already
    // zeroed out. See https://en.cppreference.com/w/cpp/memory/unique_ptr/make_unique, overload (2).
    std::memcpy(ptr, what.data(), what.size());
}

Status::Status(const Status &rhs)
    : m_data {maybe_copy_data(rhs.m_data.get())}
{
}

Status::Status(Status &&rhs) noexcept
    : m_data {std::move(rhs.m_data)}
{
}

auto Status::operator=(const Status &rhs) -> Status &
{
    if (this != &rhs) {
        m_data = maybe_copy_data(rhs.m_data.get());
    }
    return *this;
}

auto Status::operator=(Status &&rhs) noexcept -> Status &
{
    if (this != &rhs) {
        m_data = std::move(rhs.m_data);
    }
    return *this;
}

auto Status::not_found(const Slice &what) -> Status
{
    return Status {C_NotFound, what};
}

auto Status::invalid_argument(const Slice &what) -> Status
{
    return Status {C_InvalidArgument, what};
}

auto Status::system_error(const Slice &what) -> Status
{
    return Status {C_SystemError, what};
}

auto Status::logic_error(const Slice &what) -> Status
{
    return Status {C_LogicError, what};
}

auto Status::corruption(const Slice &what) -> Status
{
    return Status {C_Corruption, what};
}

auto Status::is_invalid_argument() const -> bool
{
    return !is_ok() && Code {m_data[0]} == C_InvalidArgument;
}

auto Status::is_system_error() const -> bool
{
    return !is_ok() && Code {m_data[0]} == C_SystemError;
}

auto Status::is_logic_error() const -> bool
{
    return !is_ok() && Code {m_data[0]} == C_LogicError;
}

auto Status::is_corruption() const -> bool
{
    return !is_ok() && Code {m_data[0]} == C_Corruption;
}

auto Status::is_not_found() const -> bool
{
    return !is_ok() && Code {m_data[0]} == C_NotFound;
}

auto Status::to_string() const -> std::string
{
    return {m_data ? m_data.get() + sizeof(Code) : "ok"};
}

} // namespace calicodb
