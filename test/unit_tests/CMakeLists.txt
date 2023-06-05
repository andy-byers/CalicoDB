
add_executable(unit_tests
        test_db.cpp
        test_env.cpp
        test_index.cpp
        test_pager.cpp
        test_tree.cpp
        test_utils.cpp
        unit_tests.cpp
        unit_tests.h
        ../test.cpp
        ../test.h)
target_link_libraries(unit_tests
        PUBLIC calicodb_utils gtest)
add_test(NAME unit_tests
        COMMAND unit_tests)
