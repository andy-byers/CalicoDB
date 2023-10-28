// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_DSL_H
#define CALICODB_DSL_H

#include "calicodb/slice.h"
#include "calicodb/status.h"

namespace calicodb
{

class DSLReader
{
public:
    enum EventType {
        kReadKeyValue,
        kBeginObject,
        kEndObject,
        kNumEvents
    };
    using Event = void (*)(void *, const Slice *);

    explicit DSLReader() = default;
    auto register_event(EventType type, const Event &event) -> void;
    auto read(const Slice &input, void *event_arg) -> Status;

private:
    Event m_events[kNumEvents] = {};
};

} // namespace calicodb

#endif // CALICODB_DSL_H
