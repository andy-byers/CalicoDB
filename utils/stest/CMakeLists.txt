
add_library(stest
        INTERFACE
        bounded_scenario.h
        interleaved_scenario.h
        random_scenario.h
        repeated_rule_scenario.h
        rule.h
        rule_scenario.h
        rule_sequence_scenario.h
        scenario.h
        sequence_scenario.h)
target_include_directories(stest
        INTERFACE .)