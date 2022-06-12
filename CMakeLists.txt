
cmake_minimum_required(VERSION 3.14)

SET(
    CMAKE_CXX_FLAGS_RELWITHASSERTIONS
    "-O3"
    CACHE STRING "C++ compiler flags for a RelWithAssertions build"
    FORCE
)
SET(
    CMAKE_C_FLAGS_RELWITHASSERTIONS
    "-O3"
    CACHE STRING "C compiler flags for a RelWithAssertions build"
    FORCE
)
set(CMAKE_TRY_COMPILE_TARGET_TYPE "STATIC_LIBRARY")

project(CubDB CXX)
set(CMAKE_CXX_STANDARD 17)

set(CUB_NAME cub)
set(CUB_EXAMPLES_DIR "${PROJECT_SOURCE_DIR}/examples")
set(CUB_INCLUDE_DIR "${PROJECT_SOURCE_DIR}/include")
set(CUB_SOURCE_DIR "${PROJECT_SOURCE_DIR}/src")
set(CUB_TEST_DIR "${PROJECT_SOURCE_DIR}/test")
set(CUB_WARNINGS -Wall -Wconversion -Wsign-conversion -Werror -Wextra -Wpedantic)

# Check if we have O_DIRECT, which turns off kernel page caching.
include(CheckCXXSymbolExists)
check_cxx_symbol_exists(O_DIRECT "fcntl.h" CUB_HAS_O_DIRECT)

option(CUB_BUILD_TESTS "Build the tests" ON)
option(CUB_BUILD_EXAMPLES "Build the examples" ON)

# Build the unit and integration tests.
if(CUB_BUILD_TESTS)
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

    include(FetchContent)
    FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip
    )
    FetchContent_MakeAvailable(googletest)
    include(GoogleTest)
    include(CTest)
    enable_testing()

    add_subdirectory(${CUB_TEST_DIR})
endif()

# Build the static library.
add_subdirectory(${CUB_SOURCE_DIR})

# Build the examples/use cases.
if(CUB_BUILD_EXAMPLES)
    add_subdirectory(${CUB_EXAMPLES_DIR})
endif()
