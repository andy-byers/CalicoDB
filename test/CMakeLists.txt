message("building tests")

set(TEST_TOOLS_FILES '')
file(
    GLOB_RECURSE TEST_TOOLS_FILES
    "${CCO_TEST_DIR}/tools/*.cpp"
    "${CCO_TEST_DIR}/tools/*.h"
)
add_library(test_tools "${TEST_TOOLS_FILES}")
target_link_libraries(test_tools PUBLIC "${CCO_NAME}" pthread gmock)
target_include_directories(test_tools PUBLIC "${CCO_SOURCE_DIR}" "${CCO_TEST_DIR}/tools")
target_compile_options(test_tools PUBLIC "${CCO_WARNINGS}")

add_subdirectory(unit_tests)
add_subdirectory(recovery)

if(CCO_BUILD_FUZZERS)
    add_subdirectory(fuzz)
endif()