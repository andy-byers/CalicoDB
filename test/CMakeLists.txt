
# Fetch gtest source code.
include(FetchContent)
FetchContent_Declare(googletest URL "https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip")
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)
include(GoogleTest)
enable_testing()

set(
    TEST_TOOLS
    ${PROJECT_SOURCE_DIR}/test/tools/fakes.cpp
    ${PROJECT_SOURCE_DIR}/test/tools/fakes.h
    ${PROJECT_SOURCE_DIR}/test/tools/random.cpp
    ${PROJECT_SOURCE_DIR}/test/tools/random.h
    ${PROJECT_SOURCE_DIR}/test/tools/tools.cpp
    ${PROJECT_SOURCE_DIR}/test/tools/tools.h
)

function(build_test NAME)
    set(TEST_FILES "")
    file(GLOB_RECURSE TEST_FILES "*.cpp" "*.h")
    add_executable(${NAME} ${TEST_FILES} ${TEST_TOOLS})
    target_link_libraries(${NAME} ${CUB_NAME} gmock gtest pthread)
    target_include_directories(${NAME} PRIVATE "${PROJECT_SOURCE_DIR}/test/tools")
    target_include_directories(${NAME} PRIVATE "${CUB_SOURCE_DIR}")
    target_include_directories(${NAME} PUBLIC "${CUB_INCLUDE_DIR}")
    set_target_properties(${NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CUB_TEST_DIR}/build")
endfunction()

add_subdirectory(benchmark)
add_subdirectory(fuzz)
add_subdirectory(integration)
add_subdirectory(unit)

