
set(gtest_force_shared_crt On CACHE BOOL "" FORCE)
set(INSTALL_GTEST Off CACHE INTERNAL "" FORCE)
set(INSTALL_GMOCK Off CACHE INTERNAL "" FORCE)
set(BUILD_GMOCK Off CACHE INTERNAL "" FORCE)
FetchContent_Declare(googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG release-1.11.0)
FetchContent_MakeAvailable(googletest)
include(GoogleTest)

# Documentation consistency checks.
add_subdirectory(doc_tests)

# CMake tests. Adapted from @nlohmann/json.
add_subdirectory(cmake_tests/fetch_content)
add_subdirectory(cmake_tests/add_subdirectory)

# Build the library without <string> header. <string> seems to cause transitive inclusion of a
# ton of other non-standalone standard library headers.
add_subdirectory(cmake_tests/custom_string)

function(build_test NAME)
    add_executable(${NAME} ${NAME}.cpp test.cpp test.h)
    target_link_libraries(${NAME} PRIVATE calicodb_utils gtest)
    target_include_directories(${NAME} PUBLIC "${CALICODB_SOURCE_DIR}")
    add_test(NAME ${NAME} COMMAND ${NAME})
endfunction()

build_test(test_bounds)
build_test(test_concurrency)
build_test(test_crashes)
build_test(test_db)
build_test(test_env)
build_test(test_node)
build_test(test_pager)
build_test(test_stest)
build_test(test_tree)
build_test(test_utils)

# Scenario tests using /utils/stest.
build_test(stest_db)
target_link_libraries(test_db PRIVATE stest)
target_link_libraries(test_stest PRIVATE stest)
