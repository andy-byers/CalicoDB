// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source Status::Code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/status.h"
#include "alloc.h"
#include "calicodb/slice.h"
#include "logging.h"
#include "utils.h"

namespace calicodb
{

namespace
{

auto make_inline_state(Status::Code code, Status::SubCode subc) -> char *
{
    CALICODB_EXPECT_GT(code, Status::kOK);
    CALICODB_EXPECT_LT(code, Status::kMaxCode);
    CALICODB_EXPECT_LT(subc, Status::kMaxSubCode);

    uintptr_t state = 1;
    state |= static_cast<uintptr_t>(code) << 1;
    state |= static_cast<uintptr_t>(subc) << 8;
    return reinterpret_cast<char *>(state);
}

// Return true if the given `state` is inline, false otherwise
// Uses the least-significant bit of the pointer value to distinguish between the 2 states:
// inline and heap-allocated. Inline states have the code and subcode heap in the
// pointer value, while heap-allocated states store the code and subcode in the first few
// bytes, followed by a refcount and message.
auto is_inline(const char *state) -> bool
{
    static constexpr uintptr_t kInlineBit = 0x0001;
    return reinterpret_cast<uintptr_t>(state) & kInlineBit;
}

// Return true if the given `state` is allocated on the heap, false otherwise
auto is_heap(const char *state) -> bool
{
    return state && !is_inline(state);
}

auto inline_code(const char *state) -> Status::Code
{
    static constexpr uintptr_t kCodeMask = 0x00FE;
    return static_cast<Status::Code>(
        (reinterpret_cast<uintptr_t>(state) & kCodeMask) >> 1);
}

auto inline_subcode(const char *state) -> Status::SubCode
{
    static constexpr uintptr_t kSubCodeMask = 0xFF00;
    return static_cast<Status::SubCode>(
        (reinterpret_cast<uintptr_t>(state) & kSubCodeMask) >> 8);
}

using HeapRefCount = uint16_t;

auto heap_code(const char *state) -> Status::Code
{
    return static_cast<Status::Code>(state[sizeof(HeapRefCount)]);
}

auto heap_subcode(const char *state) -> Status::SubCode
{
    return static_cast<Status::SubCode>(state[sizeof(HeapRefCount) + 1]);
}

auto heap_refcount_ptr(char *state) -> HeapRefCount *
{
    return reinterpret_cast<HeapRefCount *>(state);
}

auto heap_message(const char *state) -> const char *
{
    return state + sizeof(HeapRefCount) + 2;
}

auto incref(char *state) -> int
{
    if (is_heap(state)) {
        if (*heap_refcount_ptr(state) == UINT16_MAX) {
            return -1;
        }
        ++*heap_refcount_ptr(state);
    }
    return 0;
}

auto decref(char *state) -> void
{
    if (is_heap(state)) {
        CALICODB_EXPECT_GT(*heap_refcount_ptr(state), 0);
        if (--*heap_refcount_ptr(state) == 0) {
            Alloc::deallocate(state);
        }
    }
}

} // namespace

Status::Status(Code code, SubCode subc)
    : m_state(make_inline_state(code, subc))
{
}

Status::~Status()
{
    decref(m_state);
}

Status::Status(const Status &rhs)
    : m_state(rhs.m_state)
{
    if (incref(m_state)) {
        m_state = make_inline_state(rhs.code(), rhs.subcode());
    }
}

auto Status::operator=(const Status &rhs) -> Status &
{
    if (&rhs != this) {
        decref(m_state);
        m_state = incref(rhs.m_state)
                      ? make_inline_state(rhs.code(), rhs.subcode())
                      : rhs.m_state;
    }
    return *this;
}

Status::Status(Status &&rhs) noexcept
    : m_state(exchange(rhs.m_state, nullptr))
{
}

auto Status::operator=(Status &&rhs) noexcept -> Status &
{
    auto *state = exchange(m_state, rhs.m_state);
    rhs.m_state = state;
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
    if (is_heap(m_state)) {
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

auto Status::invalid_argument(const char *msg) -> Status
{
    return StatusBuilder(kInvalidArgument)
        .append(msg)
        .build();
}

auto Status::not_supported(const char *msg) -> Status
{
    return StatusBuilder(kNotSupported)
        .append(msg)
        .build();
}

auto Status::corruption(const char *msg) -> Status
{
    return StatusBuilder(kCorruption)
        .append(msg)
        .build();
}

auto Status::not_found(const char *msg) -> Status
{
    return StatusBuilder(kNotFound)
        .append(msg)
        .build();
}

auto Status::io_error(const char *msg) -> Status
{
    return StatusBuilder(kIOError)
        .append(msg)
        .build();
}

auto Status::busy(const char *msg) -> Status
{
    return StatusBuilder(kBusy)
        .append(msg)
        .build();
}

auto Status::aborted(const char *msg) -> Status
{
    return StatusBuilder(kAborted)
        .append(msg)
        .build();
}

auto Status::retry(const char *msg) -> Status
{
    return StatusBuilder(kBusy, kRetry)
        .append(msg)
        .build();
}

auto Status::no_memory(const char *msg) -> Status
{
    return StatusBuilder(kAborted, kNoMemory)
        .append(msg)
        .build();
}

} // namespace calicodb
