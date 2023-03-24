// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/status.h"
#include "calicodb/slice.h"

namespace calicodb
{

enum Code : char {
    kInvalidArgument = 1,
    kIOError = 2,
    kNotSupported = 3,
    kCorruption = 4,
    kNotFound = 5,
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
        std::swap(m_data, rhs.m_data);
    }
    return *this;
}

auto Status::not_found(const Slice &what) -> Status
{
    return Status {kNotFound, what};
}

auto Status::invalid_argument(const Slice &what) -> Status
{
    return Status {kInvalidArgument, what};
}

auto Status::io_error(const Slice &what) -> Status
{
    return Status {kIOError, what};
}

auto Status::not_supported(const Slice &what) -> Status
{
    return Status {kNotSupported, what};
}

auto Status::corruption(const Slice &what) -> Status
{
    return Status {kCorruption, what};
}

auto Status::is_invalid_argument() const -> bool
{
    return !is_ok() && Code {m_data[0]} == kInvalidArgument;
}

auto Status::is_io_error() const -> bool
{
    return !is_ok() && Code {m_data[0]} == kIOError;
}

auto Status::is_not_supported() const -> bool
{
    return !is_ok() && Code {m_data[0]} == kNotSupported;
}

auto Status::is_corruption() const -> bool
{
    return !is_ok() && Code {m_data[0]} == kCorruption;
}

auto Status::is_not_found() const -> bool
{
    return !is_ok() && Code {m_data[0]} == kNotFound;
}

auto Status::to_string() const -> std::string
{
    return {m_data ? m_data.get() + sizeof(Code) : "OK"};
}

} // namespace calicodb
