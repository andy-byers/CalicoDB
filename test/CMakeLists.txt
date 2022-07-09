
set(TEST_TOOLS_FILES '')
file(
    GLOB_RECURSE TEST_TOOLS_FILES
    "${CALICO_TEST_DIR}/tools/*.cpp"
    "${CALICO_TEST_DIR}/tools/*.h"
)
add_library(test_tools "${TEST_TOOLS_FILES}")
target_link_libraries(test_tools PUBLIC "${CALICO_NAME}" pthread)
target_include_directories(test_tools PUBLIC "${CALICO_SOURCE_DIR}" "${CALICO_TEST_DIR}/tools")
target_compile_options(test_tools PUBLIC "${CALICO_WARNINGS}")

add_subdirectory(benchmark)
add_subdirectory(integration)
add_subdirectory(recovery)
add_subdirectory(unit_tests)

if(CALICO_BUILD_FUZZERS)
    add_subdirectory(fuzz)
endif()
