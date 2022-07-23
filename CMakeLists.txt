cmake_minimum_required(VERSION 3.14)

SET(
    CMAKE_CXX_FLAGS_RELWITHASSERTIONS "-O3"
    CACHE STRING "C++ compiler flags for a RelWithAssertions build" FORCE
)
SET(
    CMAKE_C_FLAGS_RELWITHASSERTIONS "-O3"
    CACHE STRING "C compiler flags for a RelWithAssertions build" FORCE
)
set(CMAKE_TRY_COMPILE_TARGET_TYPE "STATIC_LIBRARY")

project(
    CalicoDB
    LANGUAGES CXX
    VERSION 0.0.1
)
set(CMAKE_CXX_STANDARD 17)

set(CCO_NAME calico)
set(CCO_EXAMPLES_DIR "${PROJECT_SOURCE_DIR}/examples")
set(CCO_INCLUDE_DIR "${PROJECT_SOURCE_DIR}/include")
set(CCO_LIBRARY_DIR "${PROJECT_SOURCE_DIR}/lib")
set(CCO_SOURCE_DIR "${PROJECT_SOURCE_DIR}/src")
set(CCO_TEST_DIR "${PROJECT_SOURCE_DIR}/test")
set(CCO_WARNINGS -Wall -Wconversion -Wsign-conversion -Werror -Wextra -Wpedantic -Wpointer-arith)

# Determine whether Calico DB is the main project, or if it was added with add_subdirectory().
set(CCO_MAIN_PROJECT Off)
if(CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
    set(CCO_MAIN_PROJECT On)
endif()

option(CCO_INSTALL "Install the library and headers" "${CCO_MAIN_PROJECT}")
option(CCO_BUILD_TESTS "Build the tests" "${CCO_MAIN_PROJECT}")
option(CCO_BUILD_BENCHMARKS "Build the benchmarks" Off)
option(CCO_BUILD_EXAMPLES "Build the examples" "${CCO_MAIN_PROJECT}")
option(CCO_BUILD_FUZZERS "Build the fuzz tests" Off)
option(CCO_FUZZER_LINK_MAIN "Link each fuzzer with a main for reproducing specific crashes" Off)
set(CCO_FUZZER_LDFLAGS "" CACHE STRING "Linker flags for the fuzz targets")

set(CCO_SOURCES '')
set(CCO_HEADERS '')
file(GLOB_RECURSE CCO_SOURCES "${CCO_SOURCE_DIR}/*/*.cpp" "${CCO_SOURCE_DIR}/*/*.h")
file(GLOB_RECURSE CCO_HEADERS "${CCO_INCLUDE_DIR}/calico/*.h")
list(APPEND CCO_SOURCES ${CCO_HEADERS})

include(GNUInstallDirs)
include(FetchContent)

FetchContent_Declare(spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG        v1.10.0
)
FetchContent_MakeAvailable(spdlog)

# Build the static library.
add_library(calico
    STATIC "${CCO_SOURCES}")
target_include_directories(calico
    PUBLIC  $<BUILD_INTERFACE:${CCO_INCLUDE_DIR}>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE "${CCO_SOURCE_DIR}")
target_compile_options(calico
    PRIVATE "${CCO_WARNINGS}")
target_link_libraries(calico
    PUBLIC  spdlog::spdlog
    PRIVATE pthread)

# Build the examples/use cases.
if(CCO_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

if(CCO_INSTALL)
    set(TARGETS_NAME ${PROJECT_NAME}Targets)
    set(INSTALL_DIR "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}")
    set(CONFIG_FILE_IN "${PROJECT_SOURCE_DIR}/cmake/${PROJECT_NAME}Config.cmake.in")
    set(CONFIG_FILE_OUT "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}Config.cmake")
    set(VERSION_FILE "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}ConfigVersion.cmake")
#
#    install(
#        TARGETS "${CCO_NAME}" spdlog
#        EXPORT "${TARGETS_NAME}"
#        RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
#        LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
#        ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
#
#    install(
#        DIRECTORY "${CCO_INCLUDE_DIR}/calico"
#        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
#    )
#    install(
#        DIRECTORY "${spdlog_SOURCE_DIR}/include/spdlog"
#        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/calico"
#    )
#
#    include(CMakePackageConfigHelpers)
#    configure_package_config_file(
#        "${CONFIG_FILE_IN}"
#        "${CONFIG_FILE_OUT}"
#        INSTALL_DESTINATION "${INSTALL_DIR}"
#    )
#    write_basic_package_version_file(
#        "${VERSION_FILE}"
#        COMPATIBILITY SameMajorVersion
#    )
#    install(
#        EXPORT "${TARGETS_NAME}"
#        NAMESPACE "${PROJECT_NAME}::"
#        DESTINATION "${INSTALL_DIR}"
#    )
#    install(
#        FILES "${CONFIG_FILE_OUT}" "${VERSION_FILE}"
#        DESTINATION "${INSTALL_DIR}"
#    )
endif()

if(CCO_BUILD_BENCHMARKS)
    # TODO: Not entirely sure why, but the following line is needed to compile with clang. See https://github.com/google/benchmark/issues/1072
    #       for more info.
#    set(BENCHMARK_ENABLE_EXCEPTIONS On)
    set(BENCHMARK_ENABLE_TESTING Off)
    FetchContent_Declare(
        googlebenchmark
        GIT_REPOSITORY https://github.com/google/benchmark.git
        GIT_TAG        v1.6.2
    )
    FetchContent_MakeAvailable(googlebenchmark)
    add_subdirectory(benchmarks)
endif()

if(CCO_BUILD_TESTS)
    set(gtest_force_shared_crt On CACHE BOOL "" FORCE)
    FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG        release-1.11.0
    )
    FetchContent_MakeAvailable(googletest)
    include(GoogleTest)
    include(CTest)
    enable_testing()
    add_subdirectory(test)
endif()