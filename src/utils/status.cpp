#include "calico/status.h"
#include "calico/slice.h"

namespace Calico {

static auto maybe_copy_data(const Byte *data) -> std::unique_ptr<Byte[]>
{
    // Status is OK, so there isn't anything to copy.
    if (data == nullptr) {
        return nullptr;
    }
    // Allocate memory for the copied message/status code.
    const auto total_size = std::char_traits<Byte>::length(data) + sizeof(Byte);
    auto copy = std::make_unique<Byte[]>(total_size);

    // Copy the status, std::make_unique<Byte[]>() will zero initialize, so we already have the null byte.
    std::memcpy(copy.get(), data, total_size - sizeof(Byte));
    return copy;
}

Status::Status(Code code, const Slice &what)
    : m_data {std::make_unique<Byte[]>(sizeof(code) + what.size() + sizeof(Byte))}
{
    auto *ptr = m_data.get();

    // The first byte holds the status type.
    *ptr++ = static_cast<Byte>(code);

    // The rest holds the message, plus a '\0'. std::make_unique<Byte[]>() performs value initialization, so the byte is already
    // zeroed out. See https://en.cppreference.com/w/cpp/memory/unique_ptr/make_unique, overload (2).
    std::memcpy(ptr, what.data(), what.size());
}

Status::Status(const Status &rhs)
    : m_data {maybe_copy_data(rhs.m_data.get())}
{}

Status::Status(Status &&rhs) noexcept
    : m_data {std::move(rhs.m_data)}
{}

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

auto Status::ok() -> Status
{
    return Status {};
}

auto Status::not_found(const Slice &what) -> Status
{
    return Status {Code::NOT_FOUND, what};
}

auto Status::invalid_argument(const Slice &what) -> Status
{
    return Status {Code::INVALID_ARGUMENT, what};
}

auto Status::system_error(const Slice &what) -> Status
{
    return Status {Code::SYSTEM_ERROR, what};
}

auto Status::logic_error(const Slice &what) -> Status
{
    return Status {Code::LOGIC_ERROR, what};
}

auto Status::corruption(const Slice &what) -> Status
{
    return Status {Code::CORRUPTION, what};
}

auto Status::is_invalid_argument() const -> bool
{
    return !is_ok() && Code {m_data[0]} == Code::INVALID_ARGUMENT;
}

auto Status::is_system_error() const -> bool
{
    return !is_ok() && Code {m_data[0]} == Code::SYSTEM_ERROR;
}

auto Status::is_logic_error() const -> bool
{
    return !is_ok() && Code {m_data[0]} == Code::LOGIC_ERROR;
}

auto Status::is_corruption() const -> bool
{
    return !is_ok() && Code {m_data[0]} == Code::CORRUPTION;
}

auto Status::is_not_found() const -> bool
{
    return !is_ok() && Code {m_data[0]} == Code::NOT_FOUND;
}

auto Status::is_ok() const -> bool
{
    return m_data == nullptr;
}

auto Status::what() const -> Slice
{
    return {m_data ? m_data.get() + sizeof(Code) : ""};
}

} // namespace Calico
