// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_STEST_RULE_SEQUENCE_SCENARIO_H
#define CALICODB_STEST_RULE_SEQUENCE_SCENARIO_H

#include "rule_scenario.h"
#include "sequence_scenario.h"

namespace stest
{

template <class State>
class RuleSequenceScenario : public SequenceScenario<State>
{
public:
    explicit RuleSequenceScenario(const char *name, Rule<State> **rules, size_t size)
        : SequenceScenario<State>(name, new Scenario<State> *[size], size)
    {
        auto **const scenarios = this->m_scenarios.scenarios;
        for (size_t i = 0; i < size; ++i) {
            scenarios[i] = new RuleScenario(*rules[i]);
        }
    }

    ~RuleSequenceScenario() override
    {
        const auto &array = this->m_scenarios;
        for (size_t i = 0; i < array.size; ++i) {
            delete array.scenarios[i];
        }
        delete[] array.scenarios;
    }
};

} // namespace stest

#endif // CALICODB_STEST_RULE_SEQUENCE_SCENARIO_H
