// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source Status::Code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/status.h"
#include "calicodb/slice.h"
#include "logging.h"
#include "utils.h"

namespace calicodb
{

static auto new_status_string(const Slice &msg) -> char *
{
    auto *p = new char[msg.size() + 1];
    std::memcpy(p, msg.data(), msg.size());
    p[msg.size()] = '\0';
    return p;
}

static auto copy_status_string(const char *data) -> char *
{
    if (data) {
        const auto length =
            std::char_traits<char>::length(data);
        return new_status_string(Slice(data, length));
    }
    return nullptr;
}

static constexpr const char *kSubCodeMessages[Status::kMaxSubCode] = {
    "",          // kNone
    "retry",     // kRetry
    "no memory", // kNoMemory
};

Status::Status(Code code, const Slice &msg)
    : m_state(new_status_string(msg)),
      m_code(code)
{
}

Status::Status(const Status &rhs)
    : m_state(copy_status_string(rhs.m_state)),
      m_code(rhs.m_code),
      m_subc(rhs.m_subc)
{
}

Status::Status(Status &&rhs) noexcept
    : m_state(rhs.m_state),
      m_code(rhs.m_code),
      m_subc(rhs.m_subc)
{
    rhs.m_state = nullptr;
    rhs.m_code = kOK;
    rhs.m_subc = kNone;
}

auto Status::operator=(const Status &rhs) -> Status &
{
    if (this != &rhs) {
        delete[] m_state;
        m_state = copy_status_string(rhs.m_state);
        m_code = rhs.m_code;
        m_subc = rhs.m_subc;
    }
    return *this;
}

auto Status::operator=(Status &&rhs) noexcept -> Status &
{
    std::swap(m_state, rhs.m_state);
    std::swap(m_code, rhs.m_code);
    std::swap(m_subc, rhs.m_subc);
    return *this;
}

auto Status::to_string() const -> std::string
{
    const char *type_name;
    switch (m_code) {
        case kOK:
            return "OK";
        case kInvalidArgument:
            type_name = "invalid argument: ";
            break;
        case kIOError:
            type_name = "I/O error: ";
            break;
        case kNotSupported:
            type_name = "not supported: ";
            break;
        case kCorruption:
            type_name = "corruption: ";
            break;
        case kNotFound:
            type_name = "not found: ";
            break;
        case kBusy:
            type_name = "busy: ";
            break;
        case kAborted:
            type_name = "aborted: ";
            break;
        default:
            type_name = "unrecognized: ";
            // This is not possible, so long as the constructors that take a `Code`
            // are never exposed (and the static method "constructors" are correct).
            CALICODB_EXPECT_TRUE(false && "unrecognized code");
    }
    std::string result(type_name);
    if (m_subc != kNone) {
        CALICODB_EXPECT_LT(m_subc, kMaxSubCode);
        result.append(kSubCodeMessages[m_subc]);
    } else if (m_state) {
        result.append(m_state);
    } else {
        // Clip off the ": " if there is no message.
        result.resize(result.size() - 2);
    }
    return result;
}

} // namespace calicodb
