// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_STEST_REPEATED_RULE_SCENARIO_H
#define CALICODB_STEST_REPEATED_RULE_SCENARIO_H

#include "scenario.h"

namespace stest
{

template <class State>
class RepeatedRuleScenario : public Scenario<State>
{
public:
    explicit RepeatedRuleScenario(Rule<State> &rule)
        : Scenario<State>(rule.name),
          m_rule(&rule)
    {
    }

    virtual ~RepeatedRuleScenario() = default;

protected:
    auto next_rule_scenario(State &state) -> Rule<State> * override
    {
        return m_rule->precondition(state) ? m_rule : nullptr;
    }

    [[nodiscard]] auto is_done_scenario() const -> bool override
    {
        return false;
    }

    auto reset_scenario() -> void override
    {
    }

private:
    Rule<State> *const m_rule;
};

} // namespace stest

#endif // CALICODB_STEST_REPEATED_RULE_SCENARIO_H
