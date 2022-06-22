
set(TEST_TOOLS_FILES '')
file(
    GLOB_RECURSE
    TEST_TOOLS_FILES
    "${CALICO_TEST_DIR}/tools/*.cpp"
    "${CALICO_TEST_DIR}/tools/*.h"
)
add_library(test_tools "${TEST_TOOLS_FILES}")
target_link_libraries(test_tools PUBLIC "${CALICO_NAME}" gmock pthread)
target_include_directories(test_tools PUBLIC "${CALICO_SOURCE_DIR}")
target_compile_options(test_tools PUBLIC "${CALICO_WARNINGS}")

function(build_test NAME SOURCES)
    add_executable("${NAME}" "${SOURCES}")
    target_link_libraries("${NAME}" test_tools gtest_main)
    target_include_directories("${NAME}" PRIVATE "${CALICO_TEST_DIR}/tools")
endfunction()

add_subdirectory(benchmark)
add_subdirectory(integration)
add_subdirectory(recovery)
add_subdirectory(unit_tests)

if(CALICO_BUILD_FUZZERS)
    add_subdirectory(fuzz)
endif()
