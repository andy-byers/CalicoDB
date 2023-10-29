// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_DSL_H
#define CALICODB_DSL_H

#include "calicodb/slice.h"
#include "calicodb/status.h"

namespace calicodb
{

enum Event {
    kEventValueString,
    kEventValueNumber,
    kEventValueBoolean,
    kEventValueNull,
    kEventBeginObject,
    kEventEndObject,
    kEventBeginArray,
    kEventEndArray,
    kEventKey, // Special event for object key
    kEventCount
};

union Value {
    void *null = nullptr;
    bool boolean;
    double number;
    Slice string;
};

using Action = void (*)(void *, const Value *);

class DSLReader
{
public:
    explicit DSLReader() = default;
    auto register_action(Event event, const Action &action) -> void;
    auto read(const Slice &input, void *action_arg) -> Status;

private:
    auto dispatch(Event event, void *action_arg, const Value *value) -> void;

    Action m_actions[kEventCount] = {};
};

} // namespace calicodb

#endif // CALICODB_DSL_H
