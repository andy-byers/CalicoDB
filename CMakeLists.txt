
cmake_minimum_required(VERSION 3.14)

message(${CMAKE_BUILD_TYPE})
set(CUB_DEBUG_FLAGS "-Wall -Wconversion -Werror -Wextra -Wpedantic -g3 -O0")
set(CMAKE_CXX_FLAGS_DEBUG ${CUB_DEBUG_FLAGS} CACHE STRING "")

project(CubDB CXX)
set(CMAKE_CXX_STANDARD 17)

option(CUB_BUILD_ASSERTIONS "Build with assertions" ON)
option(CUB_BUILD_TESTS "Build the tests" ON)

if (NOT ${CUB_BUILD_ASSERTIONS})
    add_compile_definitions(CUB_NDEBUG=1)
endif()

set(CUB_NAME cub)
set(CUB_OUTPUT_DIR "${PROJECT_SOURCE_DIR}/build")
set(CUB_EXAMPLES_DIR "${PROJECT_SOURCE_DIR}/examples")
set(CUB_INCLUDE_DIR "${PROJECT_SOURCE_DIR}/include")
set(CUB_SOURCE_DIR "${PROJECT_SOURCE_DIR}/src")
set(CUB_TEST_DIR "${PROJECT_SOURCE_DIR}/test")

# Check if we have O_DIRECT, which turns off kernel page caching.
include(CheckCXXSymbolExists)
check_cxx_symbol_exists(O_DIRECT "fcntl.h" CUB_HAS_O_DIRECT)

# Build the static library.
add_subdirectory(src)

# Build the unit_tests and integration tests.
if(CUB_BUILD_TESTS)
    add_subdirectory(${CUB_TEST_DIR})
endif()

# Build the examples/use cases.
if(CUB_BUILD_EXAMPLES)
    add_subdirectory(${CUB_EXAMPLES_DIR})
endif()
