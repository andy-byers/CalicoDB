// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/status.h"
#include "calicodb/slice.h"
#include "utils.h"

namespace calicodb
{

enum Code : char {
    kOK,
    kInvalidArgument,
    kIOError,
    kNotSupported,
    kCorruption,
    kNotFound,
    kBusy,
};

static auto new_status_string(const char *data, Code code) -> char *
{
    CALICODB_EXPECT_TRUE(data);
    // "data" doesn't need to be null-terminated, since it may come from a slice. It
    // should point to a human-readable message to store in the status.
    const auto data_size =
        sizeof(Code) + std::char_traits<char>::length(data) + 1;
    auto *p = new char[data_size]();
    std::memcpy(p + sizeof(Code), data, data_size - 1);
    p[0] = code;
    return p;
}

static auto copy_status_string(const char *data) -> char *
{
    if (data) {
        // Allocate memory for the copied message/status code. "data" should be the
        // "m_data" member of another Status, meaning it points to a null-terminated
        // string which includes the status code at the front.
        const auto data_size =
            std::char_traits<char>::length(data) + 1;
        auto *p = new char[data_size]();
        std::memcpy(p, data, data_size - 1);
        return p;
    }
    return nullptr;
}

Status::Status(char code, const Slice &what)
    : m_data(new_status_string(what.data(), Code{code}))
{
}

Status::Status(const Status &rhs)
    : m_data(copy_status_string(rhs.m_data))
{
}

Status::Status(Status &&rhs) noexcept
    : m_data(rhs.m_data)
{
    rhs.m_data = nullptr;
}

Status::~Status()
{
    delete[] m_data;
}

auto Status::operator=(const Status &rhs) -> Status &
{
    if (this != &rhs) {
        delete[] m_data;
        m_data = copy_status_string(rhs.m_data);
    }
    return *this;
}

auto Status::operator=(Status &&rhs) noexcept -> Status &
{
    std::swap(m_data, rhs.m_data);
    return *this;
}

auto Status::busy(const Slice &what) -> Status
{
    return Status(kBusy, what);
}

auto Status::not_found(const Slice &what) -> Status
{
    return Status(kNotFound, what);
}

auto Status::invalid_argument(const Slice &what) -> Status
{
    return Status(kInvalidArgument, what);
}

auto Status::io_error(const Slice &what) -> Status
{
    return Status(kIOError, what);
}

auto Status::not_supported(const Slice &what) -> Status
{
    return Status(kNotSupported, what);
}

auto Status::corruption(const Slice &what) -> Status
{
    return Status(kCorruption, what);
}

auto Status::is_invalid_argument() const -> bool
{
    return !is_ok() && Code{m_data[0]} == kInvalidArgument;
}

auto Status::is_io_error() const -> bool
{
    return !is_ok() && Code{m_data[0]} == kIOError;
}

auto Status::is_not_supported() const -> bool
{
    return !is_ok() && Code{m_data[0]} == kNotSupported;
}

auto Status::is_corruption() const -> bool
{
    return !is_ok() && Code{m_data[0]} == kCorruption;
}

auto Status::is_not_found() const -> bool
{
    return !is_ok() && Code{m_data[0]} == kNotFound;
}

auto Status::is_busy() const -> bool
{
    return !is_ok() && Code{m_data[0]} == kBusy;
}

auto Status::to_string() const -> std::string
{
    return {m_data ? m_data + sizeof(Code) : "OK"};
}

} // namespace calicodb
