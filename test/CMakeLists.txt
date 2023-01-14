
set(gtest_force_shared_crt On CACHE BOOL "" FORCE)
set(INSTALL_GTEST Off CACHE INTERNAL "" FORCE)
set(INSTALL_GMOCK Off CACHE INTERNAL "" FORCE)
set(BUILD_GMOCK Off CACHE INTERNAL "" FORCE)
FetchContent_Declare(googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG release-1.11.0)
FetchContent_MakeAvailable(googletest)
include(GoogleTest)

set(TEST_TOOLS_FILES '')
file(
    GLOB_RECURSE TEST_TOOLS_FILES
    "${CALICO_TEST_DIR}/tools/*.cpp"
    "${CALICO_TEST_DIR}/tools/*.h"
)
add_library(test_tools "${TEST_TOOLS_FILES}")
target_link_libraries(test_tools PUBLIC "${CALICO_NAME}" pthread spdlog)
target_include_directories(test_tools PUBLIC "${CALICO_SOURCE_DIR}" "${CALICO_TEST_DIR}/tools")
target_compile_options(test_tools PUBLIC "${CALICO_WARNINGS}")

add_subdirectory(unit_tests)
add_subdirectory(recovery)
