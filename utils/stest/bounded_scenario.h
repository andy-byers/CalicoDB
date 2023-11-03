// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_STEST_BOUNDED_SCENARIO_H
#define CALICODB_STEST_BOUNDED_SCENARIO_H

#include "scenario.h"

namespace stest
{

template <class State>
class BoundedScenario : public ConditionalScenario<State>
{
public:
    explicit BoundedScenario(const char *const name, Scenario<State> &scenario, size_t bound)
        : ConditionalScenario<State>(name, scenario),
          m_bound(bound),
          m_steps(0)
    {
    }

    ~BoundedScenario() override = default;

protected:
    [[nodiscard]] auto condition_conditional(const State &) const -> bool override
    {
        return m_steps < m_bound;
    }

    void next_rule_conditional(const Rule<State> *next_rule) override
    {
        m_steps += next_rule != nullptr;
    }

    void reset_conditional() override
    {
        m_steps = 0;
    }

private:
    const size_t m_bound;
    size_t m_steps;
};

} // namespace stest

#endif // CALICODB_STEST_BOUNDED_SCENARIO_H
