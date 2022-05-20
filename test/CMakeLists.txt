
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

add_subdirectory(integration)
add_subdirectory(unit)

