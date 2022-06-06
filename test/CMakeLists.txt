
set(TEST_TOOLS '')
file(
    GLOB_RECURSE
    TEST_TOOLS
    "${CUB_TEST_DIR}/tools/*.cpp"
    "${CUB_TEST_DIR}/tools/*.h"
)

function(build_test NAME SOURCES)
    add_executable("${NAME}" "${SOURCES}" "${TEST_TOOLS}")
    target_link_libraries("${NAME}" "${CUB_NAME}" gmock gtest_main pthread)
    target_include_directories("${NAME}" PRIVATE "${CUB_TEST_DIR}/tools")
    target_include_directories("${NAME}" PRIVATE "${CUB_SOURCE_DIR}")
    target_include_directories("${NAME}" PRIVATE "${CUB_INCLUDE_DIR}")
    target_compile_options("${NAME}" PRIVATE "${CUB_WARNINGS}")
endfunction()

add_subdirectory(benchmark)
add_subdirectory(fuzz)
add_subdirectory(integration)
add_subdirectory(unit_tests)