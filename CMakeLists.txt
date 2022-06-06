
cmake_minimum_required(VERSION 3.14)

project(CubDB CXX)
set(CMAKE_CXX_STANDARD 17)

option(CUB_BUILD_TESTS "Build the tests" ON)
option(CUB_BUILD_ASSERTIONS "Build with assertions enabled" ON)

set(CUB_NAME cub)
set(CUB_EXAMPLES_DIR "${PROJECT_SOURCE_DIR}/examples")
set(CUB_INCLUDE_DIR "${PROJECT_SOURCE_DIR}/include")
set(CUB_SOURCE_DIR "${PROJECT_SOURCE_DIR}/src")
set(CUB_TEST_DIR "${PROJECT_SOURCE_DIR}/test")
set(CUB_WARNINGS -Wall -Wconversion -Wsign-conversion -Werror -Wextra -Wpedantic)

# Check if we have O_DIRECT, which turns off kernel page caching.
include(CheckCXXSymbolExists)
check_cxx_symbol_exists(O_DIRECT "fcntl.h" CUB_HAS_O_DIRECT)

# BUILD_TYPE=Debug
# mkdir build && \
# cd ./build && \
# cmake -S . -B ./build "-DCMAKE_BUILD_TYPE=${BUILD_TYPE}"
# cmake --build ./build --config "${BUILD_TYPE}"
# (cd ./build && ctest -VV -C "${BUILD_TYPE}")

#add_compile_definitions(
#    $<$<CONFIG:Debug>:-DDEBUG_LEVEL=2>
#    $<$<CONFIG:Release>:-DDEBUG_LEVEL=0>
#)
# Build the unit and integration tests.
if(CUB_BUILD_TESTS)
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    set(CUB_BUILD_ASSERTIONS ON)

    include(FetchContent)
    FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG        release-1.11.0
    )
    FetchContent_MakeAvailable(googletest)
    include(GoogleTest)
    include(CTest)
    enable_testing()

    add_subdirectory(${CUB_TEST_DIR})
endif()

if(CUB_BUILD_ASSERTIONS)
    add_compile_definitions(CUB_BUILD_ASSERTIONS)
endif()

# Build the static library.
add_subdirectory(src)



# Build the examples/use cases.
if(CUB_BUILD_EXAMPLES)
    add_subdirectory(${CUB_EXAMPLES_DIR})
endif()
