cmake_minimum_required(VERSION 3.14)

set(CMAKE_CXX_FLAGS_RELWITHASSERTIONS "-O3"
    CACHE STRING "C++ compiler flags for a RelWithAssertions build" FORCE)
set(CMAKE_C_FLAGS_RELWITHASSERTIONS "-O3"
    CACHE STRING "C compiler flags for a RelWithAssertions build" FORCE)
set(CMAKE_TRY_COMPILE_TARGET_TYPE "STATIC_LIBRARY")

project(CalicoDB
    LANGUAGES CXX
    VERSION 0.0.1)
set(CMAKE_CXX_STANDARD 20)

set(CALICO_NAME calico)
set(CALICO_EXAMPLES_DIR "${CMAKE_CURRENT_SOURCE_DIR}/examples")
set(CALICO_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")
set(CALICO_LIBRARY_DIR "${CMAKE_CURRENT_SOURCE_DIR}/lib")
set(CALICO_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src")
set(CALICO_TEST_DIR "${CMAKE_CURRENT_SOURCE_DIR}/test")
set(CALICO_WARNINGS -Wall -Wconversion -Wsign-conversion -Werror -Wextra -Wpedantic -Wpointer-arith)

include(GNUInstallDirs)

# Download the library dependencies.
include(FetchContent)
FetchContent_Declare(spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG v1.10.0)
FetchContent_Declare(expected
    GIT_REPOSITORY https://github.com/TartanLlama/expected.git
    GIT_TAG v1.0.0)
set(SPDLOG_NO_EXCEPTIONS On CACHE INTERNAL "")
set(EXPECTED_ENABLE_TESTS Off CACHE INTERNAL "")
set(EXPECTED_BUILD_PACKAGE Off CACHE INTERNAL "")
FetchContent_MakeAvailable(spdlog expected)

# Compile the library.
add_subdirectory(src)

if(CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
    option(CALICO_BUILD_BENCHMARKS "Build the benchmarks" On)
    option(CALICO_BUILD_EXAMPLES "Build the examples" On)
    option(CALICO_BUILD_TESTS "Build the tests" On)

    if(CALICO_BUILD_BENCHMARKS)
        add_subdirectory(benchmarks)
    endif()

    if(CALICO_BUILD_EXAMPLES)
        add_subdirectory(examples)
    endif()

    if(CALICO_BUILD_TESTS)
        include(CTest)
        enable_testing()
        add_subdirectory(test)
    endif()

    # TODO: This installs spdlog alongside calico. Is that what we want?
    set(TARGETS_NAME ${PROJECT_NAME}Targets)
    set(INSTALL_DIR "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}")
    set(CONFIG_FILE_IN "${PROJECT_SOURCE_DIR}/cmake/${PROJECT_NAME}Config.cmake.in")
    set(CONFIG_FILE_OUT "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}Config.cmake")
    set(VERSION_FILE "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}ConfigVersion.cmake")

    install(TARGETS calico spdlog
        EXPORT "${TARGETS_NAME}"
        RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
        LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
        ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}")
    install(DIRECTORY "${CALICO_INCLUDE_DIR}/calico"
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
    install(DIRECTORY "${spdlog_SOURCE_DIR}/include/spdlog"
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")

    include(CMakePackageConfigHelpers)
    configure_package_config_file(
        "${CONFIG_FILE_IN}" "${CONFIG_FILE_OUT}"
        INSTALL_DESTINATION "${INSTALL_DIR}")
    write_basic_package_version_file("${VERSION_FILE}"
        COMPATIBILITY SameMajorVersion)
    install(EXPORT "${TARGETS_NAME}"
        NAMESPACE "${PROJECT_NAME}::"
        DESTINATION "${INSTALL_DIR}")
    install(FILES "${CONFIG_FILE_OUT}" "${VERSION_FILE}"
        DESTINATION "${INSTALL_DIR}")
endif()

