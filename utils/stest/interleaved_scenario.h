// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_STEST_REPEATED_SCENARIO_H
#define CALICODB_STEST_REPEATED_SCENARIO_H

#include <cstring>
#include "scenario.h"

namespace stest
{

template<class State>
class InterleavedScenario : public Scenario<State>
{
public:
    explicit InterleavedScenario(const char *name, Scenario<State> **scenarios, size_t size)
        : Scenario<State>(name),
          m_scenarios(scenarios, size),
          m_seen(new bool[size])
    {
    }

    ~InterleavedScenario() override = default;

protected:
    auto reset_scenario() -> void override
    {
        m_scenarios.reset();
    }

    auto next_rule_scenario(State &state) -> Rule<State> * override
    {
        std::memset(m_seen, 0, m_scenarios.size * sizeof *m_seen);

        Rule<State> *rule = nullptr;
        size_t num_iterations = 0;
        (void)num_iterations;

        for (size_t num_seen = 0; num_seen < m_scenarios.size;) {
            assert(num_iterations++ < 0xFF'FF'FF'FF);
            const auto i = rand() % m_scenarios.size;
            if (m_seen[i]) {
                continue;
            }
            rule = m_scenarios
                       .scenarios[i]
                       ->next_rule(state);
            if (rule != nullptr) {
                break;
            }
            m_seen[i] = true;
            ++num_seen;
        }
        return rule;
    }

    [[nodiscard]] auto is_done_scenario() const -> bool override
    {
        for (size_t i = 0; i < m_scenarios.size; ++i) {
            assert(m_scenarios.scenarios[i]);
            if (!m_scenarios.scenarios[i]->is_done()) {
                return false;
            }
        }
        return true;
    }

    ScenarioArray<State> m_scenarios;
    bool *m_seen;
};

} // namespace stest

#endif // CALICODB_STEST_REPEATED_SCENARIO_H
