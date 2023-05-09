
add_executable(unit_tests
        test_db.cpp
        test_env.cpp
        test_index.cpp
        test_pager.cpp
        test_recovery.cpp
        test_table.cpp
        test_tree.cpp
        test_utils.cpp
        unit_tests.cpp
        unit_tests.h )
target_link_libraries(unit_tests
        PUBLIC calicodb_tools gtest_main)
add_test(NAME unit_tests
        COMMAND unit_tests)

