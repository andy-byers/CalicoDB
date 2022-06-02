
# Fetch gtest source code.
include(FetchContent)
FetchContent_Declare(googletest URL "https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip")
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)
include(GoogleTest)
include(CTest)
enable_testing()

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
    target_include_directories("${NAME}" PRIVATE "${PROJECT_SOURCE_DIR}/test/tools")
    target_include_directories("${NAME}" PRIVATE "${CUB_SOURCE_DIR}")
    target_include_directories("${NAME}" PUBLIC "${CUB_INCLUDE_DIR}")
endfunction()

add_subdirectory(benchmark)
add_subdirectory(fuzz)
add_subdirectory(integration)
add_subdirectory(unit_tests)