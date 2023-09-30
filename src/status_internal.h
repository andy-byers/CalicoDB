// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source Status::Code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_STATUS_INTERNAL_H
#define CALICODB_STATUS_INTERNAL_H

#include "calicodb/slice.h"
#include "calicodb/status.h"
#include "internal.h"
#include "logging.h"
#include "mem.h"

namespace calicodb
{

struct HeapStatusHdr {
    uint16_t refs;
    Status::Code code;
    Status::SubCode subc;
};

static_assert(sizeof(HeapStatusHdr) == sizeof(uint32_t));

// Helper for creating Status objects
// On the surface, a Status can be in 1 of 2 states: success or failure. A successful status returns
// true when Status::is_ok() is called, and a failure status returns false. Failure statuses can be in
// 1 of 2 hidden states: inline or heap. Inline failure statuses keep all necessary fields packed in
// the state pointer value. They cannot contain an error message. Heap failure statuses store their
// fields and an error message on the heap. A successful status never requires a heap allocation.
//
// Heap failure status layout:
//      Offset | Size | Field
//     --------|------|----------
//      0      | 2    | Refcount
//      2      | 1    | Code
//      3      | 1    | SubCode
//      4      | N    | Message
//
// Inline failure status layout:
//     0    4    8    12
//     ABBB BBBB CCCC CCCC
//
//     Where A = Discriminator
//           B = Code
//           C = SubCode
//
// Note the fields that are unique to each failure state: Refcount and Discriminator. The Refcount
// is used to avoid allocating additional heap status states. Instead, we just increment the refcount
// each time a heap status is copied. The Discriminator is always located in the least-significant
// bit of the state pointer and serves to distinguish between heap and inline statuses. This bit will
// never be set if the pointer is set to the address of some object allocated by the allocation
// subsystem.
class StatusBuilder final
{
    StringBuilder m_builder;
    const Status m_fallback;

public:
    explicit StatusBuilder(Status::Code code, Status::SubCode subc = Status::kNone)
        : m_fallback(code, subc)
    {
        const HeapStatusHdr prototype = {1, code, subc};
        m_builder.append(Slice(reinterpret_cast<const char *>(&prototype),
                               sizeof(prototype)));
    }

    [[nodiscard]] auto append(const Slice &s) -> StatusBuilder &
    {
        m_builder.append(s);
        return *this;
    }

    [[nodiscard]] auto append(char c) -> StatusBuilder &
    {
        return append(Slice(&c, 1));
    }

    template <class... Args>
    [[nodiscard]] auto append_format(const char *fmt, Args &&...args) -> StatusBuilder &
    {
        m_builder.append_format(fmt, forward<Args>(args)...);
        return *this;
    }

    [[nodiscard]] auto append_escaped(const Slice &s) -> StatusBuilder &
    {
        m_builder.append(s);
        return *this;
    }

    auto build() -> Status
    {
        String string;
        if (m_builder.build(string)) {
            return m_fallback;
        }
        return Status(StringBuilder::release_string(
            move(string)));
    }

    template <class... Args>
    static auto invalid_argument(const char *fmt, Args &&...args) -> Status
    {
        return StatusBuilder(Status::kInvalidArgument)
            .append_format(fmt, forward<Args>(args)...)
            .build();
    }

    template <class... Args>
    static auto not_supported(const char *fmt, Args &&...args) -> Status
    {
        return StatusBuilder(Status::kNotSupported)
            .append_format(fmt, forward<Args>(args)...)
            .build();
    }

    template <class... Args>
    static auto corruption(const char *fmt, Args &&...args) -> Status
    {
        return StatusBuilder(Status::kCorruption)
            .append_format(fmt, forward<Args>(args)...)
            .build();
    }

    template <class... Args>
    static auto not_found(const char *fmt, Args &&...args) -> Status
    {
        return StatusBuilder(Status::kNotFound)
            .append_format(fmt, forward<Args>(args)...)
            .build();
    }

    template <class... Args>
    static auto io_error(const char *fmt, Args &&...args) -> Status
    {
        return StatusBuilder(Status::kIOError)
            .append_format(fmt, forward<Args>(args)...)
            .build();
    }

    template <class... Args>
    static auto busy(const char *fmt, Args &&...args) -> Status
    {
        return StatusBuilder(Status::kBusy)
            .append_format(fmt, forward<Args>(args)...)
            .build();
    }

    template <class... Args>
    static auto aborted(const char *fmt, Args &&...args) -> Status
    {
        return StatusBuilder(Status::kAborted)
            .append_format(fmt, forward<Args>(args)...)
            .build();
    }

    template <class... Args>
    static auto retry(const char *fmt, Args &&...args) -> Status
    {
        return StatusBuilder(Status::kBusy, Status::kRetry)
            .append_format(fmt, forward<Args>(args)...)
            .build();
    }

    template <class... Args>
    static auto no_memory(const char *fmt, Args &&...args) -> Status
    {
        return StatusBuilder(Status::kAborted, Status::kNoMemory)
            .append_format(fmt, forward<Args>(args)...)
            .build();
    }
};

} // namespace calicodb

#endif // CALICODB_STATUS_INTERNAL_H