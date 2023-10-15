// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_STATUS_H
#define CALICODB_STATUS_H

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
        kIncompatibleValue,
        kMaxSubCode
    };

    // Construct an OK status
    explicit Status()
        : m_state(nullptr)
    {
    }

    ~Status();

    // Create an OK status
    static auto ok() -> Status
    {
        return Status();
    }

    static auto invalid_argument(SubCode subc = kNone) -> Status
    {
        return Status(kInvalidArgument, subc);
    }

    static auto not_supported(SubCode subc = kNone) -> Status
    {
        return Status(kNotSupported, subc);
    }

    static auto corruption(SubCode subc = kNone) -> Status
    {
        return Status(kCorruption, subc);
    }

    static auto not_found(SubCode subc = kNone) -> Status
    {
        return Status(kNotFound, subc);
    }

    static auto io_error(SubCode subc = kNone) -> Status
    {
        return Status(kIOError, subc);
    }

    static auto busy(SubCode subc = kNone) -> Status
    {
        return Status(kBusy, subc);
    }

    static auto aborted(SubCode subc = kNone) -> Status
    {
        return Status(kAborted, subc);
    }

    static auto retry() -> Status
    {
        return busy(kRetry);
    }

    static auto no_memory() -> Status
    {
        return aborted(kNoMemory);
    }

    static auto incompatible_value() -> Status
    {
        return invalid_argument(kIncompatibleValue);
    }

    static auto invalid_argument(const char *msg) -> Status;
    static auto not_supported(const char *msg) -> Status;
    static auto corruption(const char *msg) -> Status;
    static auto not_found(const char *msg) -> Status;
    static auto io_error(const char *msg) -> Status;
    static auto busy(const char *msg) -> Status;
    static auto aborted(const char *msg) -> Status;
    static auto retry(const char *msg) -> Status;
    static auto no_memory(const char *msg) -> Status;
    static auto incompatible_value(const char *msg) -> Status;

    // Return true if the status is OK, false otherwise
    [[nodiscard]] auto is_ok() const -> bool
    {
        return m_state == nullptr;
    }

    [[nodiscard]] auto is_invalid_argument() const -> bool
    {
        return code() == kInvalidArgument;
    }

    [[nodiscard]] auto is_io_error() const -> bool
    {
        return code() == kIOError;
    }

    [[nodiscard]] auto is_not_supported() const -> bool
    {
        return code() == kNotSupported;
    }

    [[nodiscard]] auto is_corruption() const -> bool
    {
        return code() == kCorruption;
    }

    [[nodiscard]] auto is_not_found() const -> bool
    {
        return code() == kNotFound;
    }

    [[nodiscard]] auto is_busy() const -> bool
    {
        return code() == kBusy;
    }

    [[nodiscard]] auto is_aborted() const -> bool
    {
        return code() == kAborted;
    }

    [[nodiscard]] auto is_retry() const -> bool
    {
        return is_busy() && subcode() == kRetry;
    }

    [[nodiscard]] auto is_no_memory() const -> bool
    {
        return is_aborted() && subcode() == kNoMemory;
    }

    [[nodiscard]] auto is_incompatible_value() const -> bool
    {
        return is_invalid_argument() && subcode() == kIncompatibleValue;
    }

    [[nodiscard]] auto code() const -> Code;
    [[nodiscard]] auto subcode() const -> SubCode;
    [[nodiscard]] auto message() const -> const char *;

    auto operator==(const Status &rhs) const -> bool
    {
        return code() == rhs.code();
    }
    auto operator!=(const Status &rhs) const -> bool
    {
        return !(*this == rhs);
    }

    // Status can be copied and moved.
    Status(const Status &rhs);
    auto operator=(const Status &rhs) -> Status &;
    Status(Status &&rhs) noexcept;
    auto operator=(Status &&rhs) noexcept -> Status &;

private:
    friend class StatusBuilder;

    explicit Status(char *state)
        : m_state(state)
    {
    }

    explicit Status(Code code, SubCode subc);

    char *m_state;
};

} // namespace calicodb

#endif // CALICODB_STATUS_H
