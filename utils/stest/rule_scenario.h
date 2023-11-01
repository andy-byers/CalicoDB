// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_STEST_RULE_SCENARIO_H
#define CALICODB_STEST_RULE_SCENARIO_H

#include "scenario.h"

namespace stest
{

template <class State>
class RuleScenario : public Scenario<State>
{
public:
    explicit RuleScenario(Rule<State> &rule)
        : Scenario<State>(rule.name),
          m_rule(&rule)
    {
    }

    ~RuleScenario() override = default;

protected:
    [[nodiscard]] auto is_done_scenario() const -> bool override
    {
        return m_done;
    }

    void reset_scenario() override
    {
        m_done = false;
    }

    auto next_rule_scenario(State &state) -> Rule<State> * override
    {
        Rule<State> *rule = nullptr;
        if (!m_done && m_rule->precondition(state)) {
            rule = m_rule;
            m_done = true;
        }
        return rule;
    }

private:
    Rule<State> *const m_rule;
    bool m_done = false;
};

} // namespace stest

#endif // CALICODB_STEST_RULE_SCENARIO_H
