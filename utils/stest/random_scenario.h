// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_STEST_RANDOM_SCENARIO_H
#define CALICODB_STEST_RANDOM_SCENARIO_H

#include "interleaved_scenario.h"
#include "repeated_rule_scenario.h"

namespace stest
{

template<class State>
class RandomScenario : public InterleavedScenario<State>
{
public:
    explicit RandomScenario(const char *name, Rule<State> **rules, size_t size)
        : InterleavedScenario<State>(name, new Scenario<State> *[size], size)
    {
        for (size_t i = 0; i < size; ++i) {
            this->m_scenarios.scenarios[i] = new RepeatedRuleScenario<State>(*rules[i]);
        }
    }

    ~RandomScenario() override
    {
        auto **const scenarios = this->m_scenarios.scenarios;
        for (size_t i = 0; i < this->m_scenarios.size; ++i) {
            delete scenarios[i];
        }
        delete[] scenarios;
    }
};

} // namespace stest

#endif // CALICODB_STEST_RANDOM_SCENARIO_H
