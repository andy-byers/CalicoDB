// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source Status::Code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/status.h"
#include "calicodb/slice.h"
#include "logging.h"
#include "utils.h"

namespace calicodb
{

Status::Status(Code code, const char *msg)
    : m_state(msg),
      m_code(code)
{
}

Status::Status(Status &&rhs) noexcept
    : m_state(rhs.m_state),
      m_code(rhs.m_code),
      m_subc(rhs.m_subc)
{
    rhs.m_state = "";
    rhs.m_code = kOK;
    rhs.m_subc = kNone;
}

auto Status::operator=(Status &&rhs) noexcept -> Status &
{
    std::swap(m_state, rhs.m_state);
    std::swap(m_code, rhs.m_code);
    std::swap(m_subc, rhs.m_subc);
    return *this;
}

auto Status::type_name() const -> const char *
{
    switch (m_code) {
        case kOK:
            return "OK";
        case kInvalidArgument:
            return "invalid argument";
        case kIOError:
            return "I/O error";
        case kNotSupported:
            return "not supported";
        case kCorruption:
            return "corruption";
        case kNotFound:
            return "not found";
        case kBusy:
            return "busy";
        case kAborted:
            return "aborted";
        default:
            // This is not possible, so long as the constructors that take a `Code`
            // are never exposed (and the static method "constructors" are correct).
            CALICODB_EXPECT_TRUE(false && "unrecognized code");
            return "unrecognized code";
    }
}

auto Status::message() const -> const char *
{
    switch (m_subc) {
        case kNone:
            return m_state ? m_state : "";
        case kRetry:
            return "retry";
        case kNoMemory:
            return "no memory";
        default:
            CALICODB_EXPECT_TRUE(false && "unrecognized subcode");
            return "unrecognized subcode";
    }
}

} // namespace calicodb
