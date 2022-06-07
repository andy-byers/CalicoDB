
set(TEST_TOOLS_FILES '')
file(
    GLOB_RECURSE
    TEST_TOOLS_FILES
    "${CUB_TEST_DIR}/tools/*.cpp"
    "${CUB_TEST_DIR}/tools/*.h"
)
add_library(test_tools "${TEST_TOOLS_FILES}")
target_link_libraries(test_tools "${CUB_NAME}" gmock gtest_main pthread)
target_include_directories(test_tools PRIVATE "${CUB_SOURCE_DIR}")
target_include_directories(test_tools PRIVATE "${CUB_INCLUDE_DIR}")
target_compile_options(test_tools PRIVATE "${CUB_WARNINGS}")

function(build_test NAME SOURCES)
    add_executable("${NAME}" "${SOURCES}")
    target_link_libraries("${NAME}" "${CUB_NAME}" test_tools gmock gtest_main pthread)
    target_include_directories("${NAME}" PRIVATE "${CUB_TEST_DIR}/tools")
    target_include_directories("${NAME}" PRIVATE "${CUB_SOURCE_DIR}")
    target_include_directories("${NAME}" PRIVATE "${CUB_INCLUDE_DIR}")
    target_compile_options("${NAME}" PRIVATE "${CUB_WARNINGS}")
endfunction()

add_subdirectory(benchmark)
add_subdirectory(integration)
add_subdirectory(recovery)
add_subdirectory(unit_tests)

if(CUB_BUILD_FUZZERS)
    add_subdirectory(fuzz)
endif()