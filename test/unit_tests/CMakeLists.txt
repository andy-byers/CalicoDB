set(UNIT_TEST_FILES "")
file(GLOB_RECURSE UNIT_TEST_FILES "*.cpp" "*.h")
build_test(unit_tests "${UNIT_TEST_FILES}")
add_test(NAME unit_tests COMMAND unit_tests)