// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_STATUS_H
#define CALICODB_STATUS_H

#include "slice.h"

namespace calicodb
{

class Status final
{
public:
    enum Code : char {
        kOK,
        kInvalidArgument,
        kIOError,
        kNotSupported,
        kCorruption,
        kNotFound,
        kBusy,
        kRetry,
        kMaxCode
    };

    enum SubCode : char {
        kNone,
        kReadonly,
        kMaxSubCode
    };

    // Construct an OK status
    explicit Status() = default;

    ~Status();

    // Create an OK status
    static auto ok() -> Status
    {
        return Status();
    }

    [[nodiscard]] auto code() const -> Code
    {
        return m_code;
    }
    [[nodiscard]] auto subcode() const -> SubCode
    {
        return m_subc;
    }

    // Return true if the status is OK, false otherwise
    [[nodiscard]] auto is_ok() const -> bool
    {
        return m_code == kOK;
    }

    // Each of the methods in this block constructs a non-OK status, optionally with an
    // error message.
    [[nodiscard]] static auto invalid_argument(SubCode msg = kNone) -> Status
    {
        return Status(kInvalidArgument, msg);
    }
    [[nodiscard]] static auto invalid_argument(const Slice &msg) -> Status
    {
        return Status(kInvalidArgument, msg);
    }
    [[nodiscard]] static auto not_supported(SubCode msg = kNone) -> Status
    {
        return Status(kNotSupported, msg);
    }
    [[nodiscard]] static auto not_supported(const Slice &msg) -> Status
    {
        return Status(kNotSupported, msg);
    }
    [[nodiscard]] static auto corruption(SubCode msg = kNone) -> Status
    {
        return Status(kCorruption, msg);
    }
    [[nodiscard]] static auto corruption(const Slice &msg) -> Status
    {
        return Status(kCorruption, msg);
    }
    [[nodiscard]] static auto not_found(SubCode msg = kNone) -> Status
    {
        return Status(kNotFound, msg);
    }
    [[nodiscard]] static auto not_found(const Slice &msg) -> Status
    {
        return Status(kNotFound, msg);
    }
    [[nodiscard]] static auto io_error(SubCode msg = kNone) -> Status
    {
        return Status(kIOError, msg);
    }
    [[nodiscard]] static auto io_error(const Slice &msg) -> Status
    {
        return Status(kIOError, msg);
    }
    [[nodiscard]] static auto busy(SubCode msg = kNone) -> Status
    {
        return Status(kBusy, msg);
    }
    [[nodiscard]] static auto busy(const Slice &msg) -> Status
    {
        return Status(kBusy, msg);
    }
    [[nodiscard]] static auto retry(SubCode msg = kNone) -> Status
    {
        return Status(kRetry, msg);
    }
    [[nodiscard]] static auto retry(const Slice &msg) -> Status
    {
        return Status(kRetry, msg);
    }
    [[nodiscard]] static auto readonly() -> Status
    {
        return Status(kNotSupported, kReadonly);
    }

    [[nodiscard]] auto is_invalid_argument() const -> bool
    {
        return m_code == kInvalidArgument;
    }
    [[nodiscard]] auto is_io_error() const -> bool
    {
        return m_code == kIOError;
    }
    [[nodiscard]] auto is_not_supported() const -> bool
    {
        return m_code == kNotSupported;
    }
    [[nodiscard]] auto is_corruption() const -> bool
    {
        return m_code == kCorruption;
    }
    [[nodiscard]] auto is_not_found() const -> bool
    {
        return m_code == kNotFound;
    }
    [[nodiscard]] auto is_busy() const -> bool
    {
        return m_code == kBusy;
    }
    [[nodiscard]] auto is_retry() const -> bool
    {
        return m_code == kRetry;
    }
    [[nodiscard]] auto is_readonly() const -> bool
    {
        return m_code == kNotSupported && m_subc == kReadonly;
    }

    // Convert the status to a printable string.
    [[nodiscard]] auto to_string() const -> std::string;

    // Status can be copied and moved.
    Status(const Status &rhs);
    auto operator=(const Status &rhs) -> Status &;
    Status(Status &&rhs) noexcept;
    auto operator=(Status &&rhs) noexcept -> Status &;

private:
    explicit Status(Code code, const Slice &msg);
    explicit Status(Code code, SubCode subc = kNone)
        : m_code(code),
          m_subc(subc)
    {
    }

    const char *m_state = nullptr;
    Code m_code = kOK;
    SubCode m_subc = kNone;
};

} // namespace calicodb

#endif // CALICODB_STATUS_H
