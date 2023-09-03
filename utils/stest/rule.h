// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_STEST_TYPES_H
#define CALICODB_STEST_TYPES_H

#include <cassert>
#include <cstddef>

namespace stest
{

template <class State>
class Rule
{
public:
    const char *const name;

    explicit Rule(const char *const name)
        : name(name)
    {
    }

    virtual ~Rule() = default;

    auto apply(State &state) -> void
    {
        assert(precondition(state));
        action(state);
    }

    virtual auto precondition(const State &) -> bool = 0;

protected:
    virtual auto action(State &state) -> void = 0;
};

} // namespace stest

#endif // CALICODB_STEST_TYPES_H
