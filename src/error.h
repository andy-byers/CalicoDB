// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_ERROR_H
#define CALICODB_ERROR_H

#include "encoding.h"
#include "utils.h"

namespace calicodb
{

class ErrorState final
{
public:
    explicit ErrorState() = default;
    ErrorState(ErrorState &) = delete;
    auto operator=(ErrorState &) -> void = delete;
    ~ErrorState();

    enum ErrorCodeType : int {
        kCorruptedPage,
        kNumCodes
    };
    // va_start() doesn't allow types that undergo "default argument promotion".
    // Apparently, this is enough to make it work.
    using ErrorCode = int;

    // Write a specific type of formatted error message to an internal buffer. The
    // format string is determined by the ErrorCode parameter (see error.cpp).
    // Returns a pointer to the start on success, or nullptr on failure. The message
    // ends with a '\0', and is guaranteed to live until either this routine is
    // called again with the same ErrorCode, or ~ErrorState() is run.
    [[nodiscard]] auto format_error(ErrorCode code, ...) -> const char *;

private:
    struct {
        char *buf;
        size_t len;
    } m_errors[kNumCodes] = {};
};

} // namespace calicodb

#endif // CALICODB_ERROR_H
