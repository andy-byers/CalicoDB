// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_ERROR_H
#define CALICODB_ERROR_H

#include "encoding.h"
#include "ptr.h"
#include "utils.h"

namespace calicodb
{

// TODO: There's probably a better way to do this. At least, we need to be careful not to
//       trash pointers that are still being used. Since these messages will be assigned to
//       Status objects, returned during a transaction, we could maybe guarantee that they will
//       remain valid as long as the transaction is running. The goal is to provide detailed
//       error messages, without having to allocate memory in the Status class, since we have
//       to consider that heap allocation can fail, and we can't use exceptions...

// Produces and stores error messages using predefined format strings
// Note that each call to format_error() for a given ErrorCode will invalidate the last
// error message written for that ErrorCode (only 1 buffer is used per code).
class ErrorState final
{
public:
    explicit ErrorState() = default;
    ErrorState(ErrorState &) = delete;
    auto operator=(ErrorState &) -> void = delete;

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
    UniqueString m_errors[kNumCodes];
};

} // namespace calicodb

#endif // CALICODB_ERROR_H
