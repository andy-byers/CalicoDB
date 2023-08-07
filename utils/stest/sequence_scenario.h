// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_STEST_SEQUENCE_SCENARIO_H
#define CALICODB_STEST_SEQUENCE_SCENARIO_H

#include "scenario.h"

namespace stest
{

template<class State>
class SequenceScenario : public IteratedScenario<State>
{
public:
    explicit SequenceScenario(const char *name, Scenario<State> **scenarios, size_t size)
        : IteratedScenario<State>(name),
          m_scenarios(scenarios, size),
          m_done(false)
    {
    }

    ~SequenceScenario() override = default;

protected:
    auto next_scenario_iterated(State &) -> Scenario<State> * override
    {
        Scenario<State> *scenario = nullptr;
        if (!m_done) {
            scenario = m_scenarios.next_scenario();
        }
        if (scenario == nullptr) {
            m_done = true;
        }
        return scenario;
    }

    auto reset_iterated() -> void override
    {
        m_scenarios.reset();
        m_done = false;
    }

    [[nodiscard]] auto is_done_iterated() const -> bool override
    {
        return m_done;
    }

    ScenarioArray<State> m_scenarios;
    bool m_done;
};

} // namespace stest

#endif // CALICODB_STEST_SEQUENCE_SCENARIO_H
