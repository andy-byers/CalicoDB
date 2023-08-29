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

// Return true if the given `state` is inline, false otherwise
// Uses the least-significant bit of the pointer value to distinguish between the 2 states:
// inline and heap-allocated. Inline states have the code and subcode heap in the
// pointer value, while heap-allocated states store the code and subcode in the first few
// bytes, followed by a refcount and message.
static auto is_inline(const char *state) -> bool
{
    static constexpr std::uintptr_t kInlineBit = 0x0001;
    return reinterpret_cast<std::uintptr_t>(state) & kInlineBit;
}

// Return true if the given `state` is allocated on the heap, false otherwise
static auto is_heap(const char *state) -> bool
{
    return state && !is_inline(state);
}

static auto inline_code(const char *state) -> Status::Code
{
    static constexpr std::uintptr_t kCodeMask = 0x00FE;
    return static_cast<Status::Code>(
        (reinterpret_cast<std::uintptr_t>(state) & kCodeMask) >> 1);
}

static auto inline_subcode(const char *state) -> Status::SubCode
{
    static constexpr std::uintptr_t kSubCodeMask = 0xFF00;
    return static_cast<Status::SubCode>(
        (reinterpret_cast<std::uintptr_t>(state) & kSubCodeMask) >> 8);
}

using HeapRefCount = uint16_t;

static auto heap_code(const char *state) -> Status::Code
{
    return static_cast<Status::Code>(state[sizeof(HeapRefCount)]);
}

static auto heap_subcode(const char *state) -> Status::SubCode
{
    return static_cast<Status::SubCode>(state[sizeof(HeapRefCount) + 1]);
}

static auto heap_refcount_ptr(char *state) -> HeapRefCount *
{
    return reinterpret_cast<HeapRefCount *>(state);
}

static auto heap_message(const char *state) -> const char *
{
    return state + sizeof(HeapRefCount) + 2;
}

static auto incref(char *state) -> void
{
    if (is_heap(state)) {
        static constexpr uint16_t kMaxRefcount = std::numeric_limits<uint16_t>::max();
        CALICODB_EXPECT_LT(*heap_refcount_ptr(state), kMaxRefcount);
        ++*heap_refcount_ptr(state);
    }
}

static auto decref(char *state) -> void
{
    if (is_heap(state)) {
        CALICODB_EXPECT_GT(*heap_refcount_ptr(state), 0);
        if (--*heap_refcount_ptr(state) == 0) {
            Alloc::free(state);
        }
    }
}

Status::Status(Code code, SubCode subc)
    : m_state(nullptr)
{
    CALICODB_EXPECT_GT(code, kOK);
    CALICODB_EXPECT_LT(code, kMaxCode);
    CALICODB_EXPECT_LT(subc, kMaxSubCode);

    std::uintptr_t state = 1;
    state |= static_cast<std::uintptr_t>(code) << 1;
    state |= static_cast<std::uintptr_t>(subc) << 8;
    m_state = reinterpret_cast<char *>(state);
}

Status::~Status()
{
    decref(m_state);
}

Status::Status(const Status &rhs)
    : m_state(rhs.m_state)
{
    incref(m_state);
}

auto Status::operator=(const Status &rhs) -> Status &
{
    if (&rhs != this) {
        decref(m_state);
        incref(rhs.m_state);
        m_state = rhs.m_state;
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
    } else if (is_inline(m_state)) {
        return inline_code(m_state);
    } else {
        return heap_code(m_state);
    }
}

auto Status::subcode() const -> SubCode
{
    if (is_ok()) {
        return kNone;
    } else if (is_inline(m_state)) {
        return inline_subcode(m_state);
    } else {
        return heap_subcode(m_state);
    }
}

auto Status::message() const -> const char *
{
    if (is_ok()) {
        return "OK";
    } else if (!is_inline(m_state)) {
        return heap_message(m_state);
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
    auto fallback = StatusBuilder::inlined(code, subc);

    StringBuilder builder;
    if (StatusBuilder::start(builder, code, subc)) {
        return fallback;
    }
    if (builder.append(msg)) {
        return fallback;
    }
    return StatusBuilder::finish(std::move(builder));
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
