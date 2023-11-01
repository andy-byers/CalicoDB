// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "test.h"
#include "bounded_scenario.h"
#include "internal.h"
#include "random_scenario.h"
#include "rule_sequence_scenario.h"
#include "scenario.h"
#include "sequence_scenario.h"
#include <gtest/gtest.h>

namespace calicodb::test
{

using namespace stest;

template <size_t MaxCount, class State>
class CountingRule : public stest::Rule<State>
{
public:
    size_t runs = 0;

    explicit CountingRule(const char *name)
        : stest::Rule<State>(name)
    {
    }

    ~CountingRule() override = default;

    auto precondition(const State &state) -> bool override
    {
        return state.count < MaxCount;
    }

protected:
    void action(State &state) override
    {
        ++state.count;
        ++runs;
    }
};

TEST(STestTests, RuleSequence)
{
    struct TestState {
        size_t count = 0;
    } state;

    CountingRule<1, TestState> count_to_1("count_to_1");
    CountingRule<2, TestState> count_to_2("count_to_2");

    Rule<TestState> *rules[] = {
        &count_to_2,
        &count_to_1,
    };

    RuleSequenceScenario<TestState> scenario("rule_sequence", rules, ARRAY_SIZE(rules));

    scenario.run(state);
    ASSERT_EQ(state.count, 1);
    ASSERT_EQ(count_to_2.runs, 1);
    // Stops when the first precondition is not met for a rule in the sequence.
    ASSERT_EQ(count_to_1.runs, 0);

    scenario.run(state);
    ASSERT_EQ(state.count, 2);
    ASSERT_EQ(count_to_2.runs, 2);
    ASSERT_EQ(count_to_1.runs, 0);

    scenario.run(state);
    ASSERT_EQ(state.count, 2);
    ASSERT_EQ(count_to_2.runs, 2);
    ASSERT_EQ(count_to_1.runs, 0);
}

TEST(STestTests, ScenarioSequence)
{
    struct TestState {
        size_t count = 0;
    } state;

    CountingRule<1, TestState> count_to_1("count_to_1");
    CountingRule<2, TestState> count_to_2("count_to_2");
    CountingRule<3, TestState> count_to_3("count_to_3");
    CountingRule<4, TestState> count_to_4("count_to_4");
    CountingRule<5, TestState> count_to_5("count_to_5");

    Rule<TestState> *rules_1[] = {
        &count_to_5,
        &count_to_4, // Stop here the second time (count=4)
        &count_to_3,
    };

    Rule<TestState> *rules_2[] = {
        &count_to_2, // Stop here the first time (count=3)
        &count_to_1,
    };

    RuleSequenceScenario<TestState> rule_seq_1("rule_sequence_1", rules_1, ARRAY_SIZE(rules_1));
    RuleSequenceScenario<TestState> rule_seq_2("rule_sequence_2", rules_2, ARRAY_SIZE(rules_2));
    Scenario<TestState> *rule_seq[] = {
        &rule_seq_1,
        &rule_seq_2,
    };

    SequenceScenario<TestState> scenario("scenario_sequence", rule_seq, ARRAY_SIZE(rule_seq));

    scenario.run(state);
    ASSERT_EQ(state.count, 3);
    ASSERT_EQ(count_to_5.runs, 1);
    ASSERT_EQ(count_to_4.runs, 1);
    ASSERT_EQ(count_to_3.runs, 1);
    ASSERT_EQ(count_to_2.runs, 0);
    ASSERT_EQ(count_to_1.runs, 0);

    scenario.run(state);
    ASSERT_EQ(state.count, 4);
    ASSERT_EQ(count_to_5.runs, 2);
    ASSERT_EQ(count_to_4.runs, 1);
    ASSERT_EQ(count_to_3.runs, 1);
    ASSERT_EQ(count_to_2.runs, 0);
    ASSERT_EQ(count_to_1.runs, 0);
}

TEST(STestTests, RandomScenario)
{
    struct TestState {
        size_t count = 0;
    } state;

    CountingRule<16, TestState> a("a");
    CountingRule<32, TestState> b("b");
    CountingRule<64, TestState> c("c");

    Rule<TestState> *rules[] = {
        &a,
        &b,
        &c,
    };

    RandomScenario<TestState> scenario("random", rules, ARRAY_SIZE(rules));

    for (int i = 0; i < 2; ++i) {
        // Runs until each rule's precondition no longer holds.
        scenario.run(state);
        ASSERT_EQ(state.count, 64);
        ASSERT_EQ(state.count, a.runs + b.runs + c.runs);
    }
}

TEST(STestTests, BoundedScenario)
{
    struct TestState {
        size_t count = 0;
    } state;

    CountingRule<16, TestState> a("a");

    RepeatedRuleScenario<TestState> repeat(a);
    BoundedScenario<TestState> bounded("bounded_by_10", repeat, 10);

    bounded.run(state);
    ASSERT_EQ(state.count, 10);
    ASSERT_EQ(state.count, a.runs);

    bounded.run(state);
    ASSERT_EQ(state.count, 16);
    ASSERT_EQ(state.count, a.runs);
}

} // namespace calicodb::test
