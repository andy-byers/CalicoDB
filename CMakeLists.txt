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

project(
    CalicoDB
    LANGUAGES CXX
    VERSION 0.0.1
)
set(CMAKE_CXX_STANDARD 17)

set(CALICO_NAME calico)
set(CALICO_EXAMPLES_DIR "${PROJECT_SOURCE_DIR}/examples")
set(CALICO_INCLUDE_DIR "${PROJECT_SOURCE_DIR}/include")
set(CALICO_SOURCE_DIR "${PROJECT_SOURCE_DIR}/src")
set(CALICO_TEST_DIR "${PROJECT_SOURCE_DIR}/test")
set(CALICO_WARNINGS -Wall -Wconversion -Wsign-conversion -Werror -Wextra -Wpedantic -Wpointer-arith)

# Check if we have O_DIRECT, which turns off kernel page caching.
include(CheckCXXSymbolExists)
check_cxx_symbol_exists(O_DIRECT "fcntl.h" CALICO_HAS_O_DIRECT)

#option(CALICO_INSTALL "Install the library and headers" On)
option(CALICO_BUILD_TESTS "Build the tests" On)
option(CALICO_BUILD_EXAMPLES "Build the examples" On)
option(CALICO_USE_VALIDATORS "Use extra validation assertions" Off)

set(CALICO_SOURCES '')
set(CALICO_HEADERS '')
file(GLOB_RECURSE CALICO_SOURCES "${CALICO_SOURCE_DIR}/*/*.cpp" "${CALICO_SOURCE_DIR}/*/*.h")
file(GLOB_RECURSE CALICO_HEADERS "${CALICO_INCLUDE_DIR}/calico/*.h")
list(APPEND CALICO_SOURCES ${CALICO_HEADERS})

include(GNUInstallDirs)
include(FetchContent)

set(SPDLOG_URL https://github.com/gabime/spdlog/archive/refs/tags/v1.10.0.zip)
set(ZLIB_URL https://github.com/madler/zlib/archive/refs/tags/v1.2.12.zip)

FetchContent_Declare(
    spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG        v1.10.0
)
FetchContent_Declare(
    zlib
    GIT_REPOSITORY https://github.com/madler/zlib.git
    GIT_TAG        v1.2.12
)
FetchContent_MakeAvailable(spdlog zlib)

# Build the static library.
add_library("${CALICO_NAME}"
        STATIC "${CALICO_SOURCES}")
target_include_directories("${CALICO_NAME}"
        PRIVATE "${CALICO_SOURCE_DIR}" "${ZLIB_INCLUDE_DIRS}")
target_include_directories("${CALICO_NAME}"
        PUBLIC $<BUILD_INTERFACE:${CALICO_INCLUDE_DIR}>
               $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_compile_options("${CALICO_NAME}" PRIVATE "${CALICO_WARNINGS}")
target_link_libraries("${CALICO_NAME}" PUBLIC spdlog::spdlog zlib)

if(CALICO_USE_VALIDATORS)
    target_compile_definitions("${CALICO_NAME}" PRIVATE CALICO_USE_VALIDATORS)
endif()

# Build the examples/use cases.
if(CALICO_BUILD_EXAMPLES)
    add_subdirectory(${CALICO_EXAMPLES_DIR})
endif()

#if(CALICO_INSTALL)
#    set(TARGETS_NAME ${PROJECT_NAME}Targets)
#    set(INSTALL_DIR "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}")
#    set(CONFIG_FILE_IN "${PROJECT_SOURCE_DIR}/cmake/${PROJECT_NAME}Config.cmake.in")
#    set(CONFIG_FILE_OUT "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}Config.cmake")
#    set(VERSION_FILE "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}ConfigVersion.cmake")
#
#    install(
#        TARGETS "${CALICO_NAME}"
#        EXPORT "${TARGETS_NAME}"
#        RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
#        LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
#        ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
#    )
#    install(
#        FILES "${CALICO_HEADERS}"
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
#endif()

if(CALICO_BUILD_TESTS)
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG        release-1.11.0
    )
    FetchContent_MakeAvailable(googletest)
    include(GoogleTest)
    include(CTest)
    enable_testing()

    add_subdirectory(${CALICO_TEST_DIR})
endif()