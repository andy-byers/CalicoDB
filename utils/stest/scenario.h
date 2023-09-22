// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_STEST_SCENARIO_H
#define CALICODB_STEST_SCENARIO_H

#include "rule.h"

namespace stest
{

static FILE *s_debug_file = nullptr;

template <class State>
class Scenario
{
public:
    const char *const name;

    explicit Scenario(const char *const name)
        : name(name)
    {
    }

    virtual ~Scenario() = default;

    [[nodiscard]] auto is_done() const -> bool
    {
        return is_done_scenario();
    }

    auto reset() -> void
    {
        return reset_scenario();
    }

    auto run(State &state) -> size_t
    {
        return apply_all_rules(state);
    }

    auto next_rule(State &state) -> Rule<State> *
    {
        return is_done() ? nullptr : next_rule_scenario(state);
    }

protected:
    auto apply_rule(Rule<State> &rule, State &state) const -> void
    {
        if (s_debug_file) {
            std::fprintf(s_debug_file,
                         "[Scenario %s] Applying rule %s\n",
                         name, rule.name);
        }
        rule.apply(state);
    }

    auto apply_all_rules(State &state) -> size_t
    {
        reset();

        Rule<State> *rule;
        size_t num_steps = 0;
        while ((rule = next_rule(state))) {
            apply_rule(*rule, state);
            ++num_steps;
        }
        return num_steps;
    }

    virtual auto next_rule_scenario(State &state) -> Rule<State> * = 0;
    [[nodiscard]] virtual auto is_done_scenario() const -> bool = 0;
    virtual auto reset_scenario() -> void = 0;
};

template <class State>
class ScenarioArray final
{
public:
    Scenario<State> **const scenarios;
    const size_t size;

    explicit ScenarioArray(Scenario<State> **scenarios_, size_t size_)
        : scenarios(scenarios_),
          size(size_),
          m_sequence_index(0)
    {
        assert(scenarios && size);
    }

    auto reset() -> void
    {
        m_sequence_index = 0;
        for (size_t i = 0; i < size; ++i) {
            scenarios[i]->reset();
        }
    }

    auto next_scenario() -> Scenario<State> *
    {
        Scenario<State> *scenario = nullptr;
        if (m_sequence_index < size) {
            scenario = scenarios[m_sequence_index];
            ++m_sequence_index;
        }
        if (scenario) {
            scenario->reset();
        }
        return scenario;
    }

private:
    size_t m_sequence_index;
};

template <class State>
class IteratedScenario : public Scenario<State>
{
public:
    explicit IteratedScenario(const char *name_)
        : Scenario<State>(name_)
    {
    }

    ~IteratedScenario() override = default;

    auto next_scenario(State &state) -> Scenario<State> *
    {
        Scenario<State> *scenario = nullptr;
        if (!this->is_done()) {
            scenario = next_scenario_iterated(state);
        }
        if (scenario) {
            scenario->reset();
        }
        return scenario;
    }

protected:
    [[nodiscard]] auto is_done_scenario() const -> bool override
    {
        return is_done_iterated();
    }

    auto reset_scenario() -> void override
    {
        m_current_scenario = nullptr;
        reset_iterated();
    }

    auto next_rule_scenario(State &state) -> Rule<State> * override
    {
        Rule<State> *rule = nullptr;
        if (m_current_scenario == nullptr) {
            m_current_scenario = next_scenario(state);
        }
        if (m_current_scenario) {
            rule = m_current_scenario->next_rule(state);
        }
        while (m_current_scenario &&
               m_current_scenario->is_done() &&
               rule == nullptr) {
            m_current_scenario = next_scenario(state);
            if (m_current_scenario) {
                rule = m_current_scenario->next_rule(state);
            }
        }
        return rule;
    }

    [[nodiscard]] virtual auto is_done_iterated() const -> bool = 0;
    virtual auto next_scenario_iterated(State &state) -> Scenario<State> * = 0;
    virtual auto reset_iterated() -> void = 0;

private:
    Scenario<State> *m_current_scenario;
};

template <class State>
class ConditionalScenario : public Scenario<State>
{
public:
    explicit ConditionalScenario(const char *name_, Scenario<State> &scenario)
        : Scenario<State>(name_),
          m_scenario(&scenario)
    {
    }

    ~ConditionalScenario() override = default;

protected:
    auto reset_scenario() -> void override
    {
        m_scenario->reset();
        reset_conditional();
    }

    auto next_rule_scenario(State &state) -> Rule<State> * override
    {
        Rule<State> *rule = nullptr;
        if (condition_conditional(state)) {
            rule = m_scenario->next_rule(state);
        }
        next_rule_conditional(rule);
        return rule;
    }

    [[nodiscard]] auto is_done_scenario() const -> bool override
    {
        return m_scenario->is_done();
    }

    [[nodiscard]] virtual auto condition_conditional(const State &) const -> bool = 0;
    virtual auto next_rule_conditional(const Rule<State> *next_rule) -> void = 0;
    virtual auto reset_conditional() -> void = 0;

private:
    Scenario<State> *const m_scenario;
};

} // namespace stest

#endif // CALICODB_STEST_SCENARIO_H
