// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source Status::Code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/status.h"
#include "alloc.h"
#include "calicodb/slice.h"
#include "logging.h"
#include "utils.h"
#include <limits>

namespace calicodb
{

static auto is_compressed(const char *state) -> bool
{
    static constexpr std::uintptr_t kCompressedBit = 0x0001;
    return reinterpret_cast<std::uintptr_t>(state) & kCompressedBit;
}

static auto is_referenced(const char *state) -> bool
{
    return state && !is_compressed(state);
}

static auto compressed_code(const char *state) -> Status::Code
{
    static constexpr std::uintptr_t kCodeMask = 0x00FE;
    return static_cast<Status::Code>(
        (reinterpret_cast<std::uintptr_t>(state) & kCodeMask) >> 1);
}

static auto embedded_code(const char *state) -> Status::Code
{
    return static_cast<Status::Code>(state[0]);
}

static auto compressed_subcode(const char *state) -> Status::SubCode
{
    static constexpr std::uintptr_t kSubCodeMask = 0xFF00;
    return static_cast<Status::SubCode>(
        (reinterpret_cast<std::uintptr_t>(state) & kSubCodeMask) >> 8);
}

static auto embedded_subcode(const char *state) -> Status::SubCode
{
    return static_cast<Status::SubCode>(state[1]);
}

static auto embedded_refcount_ptr(char *state) -> uint16_t *
{
    return reinterpret_cast<uint16_t *>(state + 2);
}

static auto embedded_message(const char *state) -> const char *
{
    return state + 4;
}

Status::Status(Code code, SubCode subc)
    : m_state(nullptr)
{
    CALICODB_EXPECT_GT(code, kOK);
    CALICODB_EXPECT_LT(code, kMaxCode);
    CALICODB_EXPECT_LT(subc, kMaxSubCode);

    std::uintptr_t state = 1;
    state |= code << 1;
    state |= subc << 8;
    m_state = reinterpret_cast<char *>(state);
}

Status::Status(const Status &rhs)
    : m_state(rhs.m_state)
{
    if (is_referenced(m_state)) {
        ++*embedded_refcount_ptr(m_state);
    }
}

Status::~Status()
{
    if (is_referenced(m_state)) {
        CALICODB_EXPECT_GT(*embedded_refcount_ptr(m_state), 0);
        if (--*embedded_refcount_ptr(m_state) == 0) {
            Alloc::free(m_state);
        }
    }
}

auto Status::operator=(const Status &rhs) -> Status &
{
    if (&rhs != this) {
        m_state = rhs.m_state;
        if (is_referenced(m_state)) {
            ++*embedded_refcount_ptr(m_state);
        }
    }
    return *this;
}

Status::Status(Status &&rhs) noexcept
    : m_state(std::exchange(rhs.m_state, nullptr))
{
}

auto Status::operator=(Status &&rhs) noexcept -> Status &
{
    std::swap(m_state, rhs.m_state);
    return *this;
}

auto Status::code() const -> Code
{
    if (is_ok()) {
        return kOK;
    } else if (is_compressed(m_state)) {
        return compressed_code(m_state);
    } else {
        return embedded_code(m_state);
    }
}

auto Status::subcode() const -> SubCode
{
    if (is_ok()) {
        return kNone;
    } else if (is_compressed(m_state)) {
        return compressed_subcode(m_state);
    } else {
        return embedded_subcode(m_state);
    }
}

auto Status::message() const -> const char *
{
    if (is_ok()) {
        return "OK";
    } else if (!is_compressed(m_state)) {
        return embedded_message(m_state);
    } else if (is_retry()) {
        return "busy: retry";
    } else if (is_no_memory()) {
        return "aborted: no memory";
    }
    switch (code()) {
        case Status::kOK:
            return "OK";
        case Status::kInvalidArgument:
            return "invalid argument";
        case Status::kIOError:
            return "I/O error";
        case Status::kNotSupported:
            return "not supported";
        case Status::kCorruption:
            return "corruption";
        case Status::kNotFound:
            return "not found";
        case Status::kBusy:
            return "busy";
        case Status::kAborted:
            return "aborted";
        default:
            // This is not possible, so long as the constructors that take a `Code`
            // are never exposed (and the static method "constructors" are correct).
            CALICODB_DEBUG_TRAP;
            return "unrecognized code";
    }
}

static auto make_error(Status::Code code, Status::SubCode subc, const char *msg) -> Status
{
    // Compressed status to return if there isn't enough memory.
    auto fallback = StatusBuilder::compressed(code, subc);

    StringBuilder builder;
    if (StatusBuilder::initialize(builder, code, subc)) {
        return fallback;
    }
    if (builder.append(msg)) {
        return fallback;
    }
    return StatusBuilder::finalize(std::move(builder));
}

auto Status::invalid_argument(const char *msg) -> Status
{
    return make_error(kInvalidArgument, kNone, msg);
}

auto Status::not_supported(const char *msg) -> Status
{
    return make_error(kNotSupported, kNone, msg);
}

auto Status::corruption(const char *msg) -> Status
{
    return make_error(kCorruption, kNone, msg);
}

auto Status::not_found(const char *msg) -> Status
{
    return make_error(kNotFound, kNone, msg);
}

auto Status::io_error(const char *msg) -> Status
{
    return make_error(kIOError, kNone, msg);
}

auto Status::busy(const char *msg) -> Status
{
    return make_error(kBusy, kNone, msg);
}

auto Status::aborted(const char *msg) -> Status
{
    return make_error(kAborted, kNone, msg);
}

auto Status::retry(const char *msg) -> Status
{
    return make_error(kBusy, kRetry, msg);
}

auto Status::no_memory(const char *msg) -> Status
{
    return make_error(kAborted, kNoMemory, msg);
}

} // namespace calicodb
