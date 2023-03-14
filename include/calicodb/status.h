// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_STATUS_H
#define CALICODB_STATUS_H

#include "slice.h"
#include <memory>

namespace calicodb
{

class Status final
{
public:
    explicit Status() = default;

    // Create a status object in an OK state.
    static auto ok() -> Status
    {
        return Status {};
    }

    // Determine if a status is OK.
    [[nodiscard]] auto is_ok() const -> bool
    {
        return m_data == nullptr;
    }

    // Create a non-OK status with an error message.
    [[nodiscard]] static auto invalid_argument(const Slice &what) -> Status;
    [[nodiscard]] static auto not_supported(const Slice &what) -> Status;
    [[nodiscard]] static auto corruption(const Slice &what) -> Status;
    [[nodiscard]] static auto not_found(const Slice &what) -> Status;
    [[nodiscard]] static auto io_error(const Slice &what) -> Status;

    // Check error status type.
    [[nodiscard]] auto is_invalid_argument() const -> bool;
    [[nodiscard]] auto is_not_supported() const -> bool;
    [[nodiscard]] auto is_corruption() const -> bool;
    [[nodiscard]] auto is_not_found() const -> bool;
    [[nodiscard]] auto is_io_error() const -> bool;

    // Convert the status to a printable string.
    [[nodiscard]] auto to_string() const -> std::string;

    // Status can be copied and moved.
    Status(const Status &rhs);
    auto operator=(const Status &rhs) -> Status &;
    Status(Status &&rhs) noexcept;
    auto operator=(Status &&rhs) noexcept -> Status &;

private:
    // Construct a non-OK status.
    explicit Status(char code, const Slice &what);

    // Storage for a status code and a null-terminated message.
    std::unique_ptr<char[]> m_data;
};

static_assert(sizeof(Status) == sizeof(void *));

} // namespace calicodb

#endif // CALICODB_STATUS_H
