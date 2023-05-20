
add_executable(unit_tests
        test_db.cpp
        test_env.cpp
        test_index.cpp
        test_pager.cpp
        test_tree.cpp
        test_utils.cpp
        unit_tests.cpp
        unit_tests.h
        ../tools/harness.cpp
        ../tools/harness.h)
target_link_libraries(unit_tests
        PUBLIC calicodb_tools gtest)
add_test(NAME unit_tests
        COMMAND unit_tests)

# TODO: Move somewhere else.
add_executable(notes notes.cpp)
target_link_libraries(notes
        PUBLIC calicodb)
