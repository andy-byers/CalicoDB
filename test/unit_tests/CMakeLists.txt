set(UNIT_TEST_FILES "")
file(GLOB_RECURSE UNIT_TEST_FILES "*.cpp" "*.h")
add_executable(unit_tests "${UNIT_TEST_FILES}")
target_link_libraries(unit_tests PRIVATE test_tools gtest_main)
add_test(NAME unit_tests COMMAND unit_tests)

