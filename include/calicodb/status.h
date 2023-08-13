// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_STATUS_H
#define CALICODB_STATUS_H

#include "slice.h"

namespace calicodb
{

class [[nodiscard]] Status final
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
        kAborted,
        kMaxCode
    };

    enum SubCode : char {
        kNone,
        kRetry,
        kNoMemory,
        kMaxSubCode
    };

    // Construct an OK status
    explicit Status() = default;

    // Create an OK status
    static auto ok() -> Status
    {
        return Status();
    }

    // Each of the methods in this block constructs a non-OK status, optionally with an
    // error message.
    static auto invalid_argument(SubCode msg = kNone) -> Status
    {
        return Status(kInvalidArgument, msg);
    }
    static auto invalid_argument(const char *msg) -> Status
    {
        return Status(kInvalidArgument, msg);
    }
    static auto not_supported(SubCode msg = kNone) -> Status
    {
        return Status(kNotSupported, msg);
    }
    static auto not_supported(const char *msg) -> Status
    {
        return Status(kNotSupported, msg);
    }
    static auto corruption(SubCode msg = kNone) -> Status
    {
        return Status(kCorruption, msg);
    }
    static auto corruption(const char *msg) -> Status
    {
        return Status(kCorruption, msg);
    }
    static auto not_found(SubCode msg = kNone) -> Status
    {
        return Status(kNotFound, msg);
    }
    static auto not_found(const char *msg) -> Status
    {
        return Status(kNotFound, msg);
    }
    static auto io_error(SubCode msg = kNone) -> Status
    {
        return Status(kIOError, msg);
    }
    static auto io_error(const char *msg) -> Status
    {
        return Status(kIOError, msg);
    }
    static auto busy(SubCode msg = kNone) -> Status
    {
        return Status(kBusy, msg);
    }
    static auto busy(const char *msg) -> Status
    {
        return Status(kBusy, msg);
    }
    static auto aborted(SubCode msg = kNone) -> Status
    {
        return Status(kAborted, msg);
    }
    static auto aborted(const char *msg) -> Status
    {
        return Status(kAborted, msg);
    }

    static auto retry() -> Status
    {
        return Status(kBusy, kRetry);
    }
    static auto no_memory() -> Status
    {
        return Status(kAborted, kNoMemory);
    }

    // Return true if the status is OK, false otherwise
    [[nodiscard]] auto is_ok() const -> bool
    {
        return m_code == kOK;
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
    [[nodiscard]] auto is_aborted() const -> bool
    {
        return m_code == kAborted;
    }

    [[nodiscard]] auto is_retry() const -> bool
    {
        return m_code == kBusy && m_subc == kRetry;
    }
    [[nodiscard]] auto is_no_memory() const -> bool
    {
        return m_code == kAborted && m_subc == kNoMemory;
    }

    [[nodiscard]] auto code() const -> Code
    {
        return m_code;
    }
    [[nodiscard]] auto subcode() const -> SubCode
    {
        return m_subc;
    }

    [[nodiscard]] auto type_name() const -> const char *;
    [[nodiscard]] auto message() const -> const char *;

    auto operator==(const Status &rhs) const -> bool
    {
        return m_code == rhs.m_code;
    }
    auto operator!=(const Status &rhs) const -> bool
    {
        return !(*this == rhs);
    }

    // Status can be copied and moved.
    Status(const Status &rhs) = default;
    auto operator=(const Status &rhs) -> Status & = default;
    Status(Status &&rhs) noexcept;
    auto operator=(Status &&rhs) noexcept -> Status &;

private:
    explicit Status(Code code, const char *msg);
    explicit Status(Code code, SubCode subc = kNone)
        : m_code(code),
          m_subc(subc)
    {
    }

    const char *m_state = "";
    Code m_code = kOK;
    SubCode m_subc = kNone;
};

} // namespace calicodb

#endif // CALICODB_STATUS_H
