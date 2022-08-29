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

set(CALICO_NAME calico)
set(CALICO_EXAMPLES_DIR "${PROJECT_SOURCE_DIR}/examples")
set(CALICO_INCLUDE_DIR "${PROJECT_SOURCE_DIR}/include")
set(CALICO_LIBRARY_DIR "${PROJECT_SOURCE_DIR}/lib")
set(CALICO_SOURCE_DIR "${PROJECT_SOURCE_DIR}/src")
set(CALICO_TEST_DIR "${PROJECT_SOURCE_DIR}/test")
set(CALICO_WARNINGS -Wall -Wconversion -Wsign-conversion -Werror -Wextra -Wpedantic -Wpointer-arith)

# Determine whether Calico DB is the main project, or if it was added with add_subdirectory().
set(CALICO_MAIN_PROJECT Off)
if(CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
    set(CALICO_MAIN_PROJECT On)
endif()

option(CALICO_INSTALL "Install the library and headers" "${CALICO_MAIN_PROJECT}")
option(CALICO_BUILD_TESTS "Build the tests" "${CALICO_MAIN_PROJECT}")
option(CALICO_BUILD_EXAMPLES "Build the examples" "${CALICO_MAIN_PROJECT}")
option(CALICO_BUILD_FUZZERS "Build the fuzz tests" Off)
option(CALICO_FUZZER_LINK_MAIN "Link each fuzzer with a main for reproducing specific crashes" Off)
set(CALICO_FUZZER_LDFLAGS "" CACHE STRING "Linker flags for the fuzz targets")

set(CALICO_SOURCES '')
set(CALICO_HEADERS '')

file(GLOB_RECURSE CALICO_SOURCES "${CALICO_SOURCE_DIR}/*/*.cpp" "${CALICO_SOURCE_DIR}/*/*.h")
file(GLOB_RECURSE CALICO_HEADERS "${CALICO_INCLUDE_DIR}/calico/*.h")
list(APPEND CALICO_SOURCES "${CALICO_HEADERS}")

include(GNUInstallDirs)
include(FetchContent)

set(SPDLOG_NO_EXCEPTIONS On)
FetchContent_Declare(spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG        v1.10.0
)

#set(EXPECTED_BUILD_TESTS Off)
#FetchContent_Declare(expected
#    GIT_REPOSITORY https://github.com/TartanLlama/expected.git
#    GIT_TAG        v1.0.0
#)
FetchContent_MakeAvailable(spdlog)# expected)

# Build the static library.
add_library(calico
    STATIC "${CALICO_SOURCES}")
target_include_directories(calico
    PUBLIC  $<BUILD_INTERFACE:${CALICO_INCLUDE_DIR}>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE "${CALICO_SOURCE_DIR}")
target_compile_options(calico
    PRIVATE "${CALICO_WARNINGS}")# -fno-rtti) # TODO: Get Calico DB to compile without exceptions, using "-fno-exceptions".
target_compile_definitions(calico
    PUBLIC CALICO_VERSION_MAJOR=${PROJECT_VERSION_MAJOR}
           CALICO_VERSION_MINOR=${PROJECT_VERSION_MINOR}
           CALICO_VERSION_PATCH=${PROJECT_VERSION_PATCH})
target_link_libraries(calico
    PUBLIC spdlog::spdlog
    PRIVATE pthread)

# Build the examples/use cases.
if(CALICO_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()


if(CALICO_BUILD_TESTS)
    set(gtest_force_shared_crt On CACHE BOOL "" FORCE)
    set(INSTALL_GTEST Off)
    set(INSTALL_GMOCK Off)
    set(BUILD_GMOCK On)
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

if(CALICO_INSTALL)
    set(TARGETS_NAME ${PROJECT_NAME}Targets)
    set(INSTALL_DIR "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}")
    set(CONFIG_FILE_IN "${PROJECT_SOURCE_DIR}/cmake/${PROJECT_NAME}Config.cmake.in")
    set(CONFIG_FILE_OUT "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}Config.cmake")
    set(VERSION_FILE "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}ConfigVersion.cmake")

    # set_target_properties()?

    install(
        TARGETS             calico spdlog
        EXPORT              "${TARGETS_NAME}"
        RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
        LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
        ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    )
    install(
        FILES       "${CALICO_INCLUDE_DIR}/calico/bytes.h"
                    "${CALICO_INCLUDE_DIR}/calico/calico.h"
                    "${CALICO_INCLUDE_DIR}/calico/common.h"
                    "${CALICO_INCLUDE_DIR}/calico/cursor.h"
                    "${CALICO_INCLUDE_DIR}/calico/database.h"
                    "${CALICO_INCLUDE_DIR}/calico/info.h"
                    "${CALICO_INCLUDE_DIR}/calico/options.h"
                    "${CALICO_INCLUDE_DIR}/calico/status.h"
                    "${CALICO_INCLUDE_DIR}/calico/store.h"
                    "${CALICO_INCLUDE_DIR}/calico/transaction.h"
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
    )
    install(
        DIRECTORY "${spdlog_SOURCE_DIR}/include/spdlog"
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
    )

    include(CMakePackageConfigHelpers)
    configure_package_config_file(
        "${CONFIG_FILE_IN}" "${CONFIG_FILE_OUT}"
        INSTALL_DESTINATION "${INSTALL_DIR}"
    )
    write_basic_package_version_file("${VERSION_FILE}"
        COMPATIBILITY SameMajorVersion
    )
    install(
        EXPORT      "${TARGETS_NAME}"
        NAMESPACE   "${PROJECT_NAME}::"
        DESTINATION "${INSTALL_DIR}"
    )
    install(
        FILES       "${CONFIG_FILE_OUT}"
                    "${VERSION_FILE}"
        DESTINATION "${INSTALL_DIR}"
    )
endif()
