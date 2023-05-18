cmake_minimum_required(VERSION 3.14)

set(CMAKE_CXX_FLAGS_RELWITHASSERTIONS "-O3"
        CACHE STRING "C++ compiler flags for a RelWithAssertions build" FORCE)
set(CMAKE_C_FLAGS_RELWITHASSERTIONS "-O3"
        CACHE STRING "C compiler flags for a RelWithAssertions build" FORCE)
set(CMAKE_TRY_COMPILE_TARGET_TYPE "STATIC_LIBRARY")

project(calicodb
        LANGUAGES CXX
        VERSION 0.0.1)
set(CMAKE_CXX_STANDARD 17)

set(CALICODB_INCLUDE_DIR "${PROJECT_SOURCE_DIR}/include")
set(CALICODB_SOURCE_DIR "${PROJECT_SOURCE_DIR}/src")

set(MAIN_PROJECT Off)
if (CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
    set(MAIN_PROJECT On)
endif()

option(CALICODB_BuildTests "Build the tests" ${MAIN_PROJECT})
option(CALICODB_Install "Install the CMake targets during the install step" ${MAIN_PROJECT})

# CalicoDB is built without exceptions or RTTI. The tests are allowed to use RTTI though.
set(CALICODB_OPTIONS -fno-exceptions)
if(NOT CALICODB_BuildTests)
    list(APPEND CALICODB_OPTIONS -fno-rtti)
endif()

# All applicable warnings from @lefticus/cpp-best-practices.
set(COMPILER_VENDOR ${CMAKE_CXX_COMPILER_ID})
if(COMPILER_VENDOR STREQUAL MSVC)
    message(FATAL_ERROR 'Visual Studio is not yet supported')
else()
    list(APPEND CALICODB_OPTIONS -Werror)
    list(APPEND CALICODB_OPTIONS -Wall)
    list(APPEND CALICODB_OPTIONS -Wextra)
    list(APPEND CALICODB_OPTIONS -Wshadow)
    list(APPEND CALICODB_OPTIONS -Wnon-virtual-dtor)
    list(APPEND CALICODB_OPTIONS -Wpedantic)
    list(APPEND CALICODB_OPTIONS -Wold-style-cast)
    list(APPEND CALICODB_OPTIONS -Wcast-align)
    list(APPEND CALICODB_OPTIONS -Wunused)
    list(APPEND CALICODB_OPTIONS -Woverloaded-virtual)
    list(APPEND CALICODB_OPTIONS -Wdouble-promotion)
    list(APPEND CALICODB_OPTIONS -Wimplicit-fallthrough)
    list(APPEND CALICODB_OPTIONS -Wno-unused-variable)

    if(COMPILER_VENDOR STREQUAL GNU)
        list(APPEND CALICODB_OPTIONS -Wconversion)
        list(APPEND CALICODB_OPTIONS -Wsign-conversion)
        list(APPEND CALICODB_OPTIONS -Wduplicated-cond)
        list(APPEND CALICODB_OPTIONS -Wduplicated-branches)
        list(APPEND CALICODB_OPTIONS -Wlogical-op)
        list(APPEND CALICODB_OPTIONS -Wnull-dereference)
        list(APPEND CALICODB_OPTIONS -Wuseless-cast)
    endif()
endif()

file(GLOB_RECURSE
        CALICODB_SOURCES
        "${CALICODB_SOURCE_DIR}/*.cpp"
        "${CALICODB_SOURCE_DIR}/*.h")
add_library(calicodb
        "${CALICODB_INCLUDE_DIR}/calicodb/cursor.h"
        "${CALICODB_INCLUDE_DIR}/calicodb/db.h"
        "${CALICODB_INCLUDE_DIR}/calicodb/env.h"
        "${CALICODB_INCLUDE_DIR}/calicodb/options.h"
        "${CALICODB_INCLUDE_DIR}/calicodb/slice.h"
        "${CALICODB_INCLUDE_DIR}/calicodb/status.h"
        "${CALICODB_SOURCES}")
target_include_directories(calicodb
        PUBLIC  $<BUILD_INTERFACE:${CALICODB_INCLUDE_DIR}>
                $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        PRIVATE "${CALICODB_SOURCE_DIR}")
target_compile_options(calicodb
        PRIVATE "${CALICODB_OPTIONS}")
target_compile_definitions(calicodb
        PUBLIC CALICODB_VERSION_MAJOR=${PROJECT_VERSION_MAJOR}
               CALICODB_VERSION_MINOR=${PROJECT_VERSION_MINOR}
               CALICODB_VERSION_PATCH=${PROJECT_VERSION_PATCH})
if(${CALICODB_BuildTests})
    target_compile_definitions(calicodb
            PRIVATE CALICODB_TEST)
endif()

if(CALICODB_BuildTests)
    include(FetchContent)
    include(CTest)
    enable_testing()
    add_subdirectory(test)
endif()

include(GNUInstallDirs)

set(TARGETS_NAME ${PROJECT_NAME}Targets)
set(INSTALL_DIR "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}")
set(CONFIG_FILE_IN "${PROJECT_SOURCE_DIR}/cmake/${PROJECT_NAME}Config.cmake.in")
set(CONFIG_FILE_OUT "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}Config.cmake")
set(VERSION_FILE "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}ConfigVersion.cmake")

include(CMakePackageConfigHelpers)
configure_package_config_file(
        "${CONFIG_FILE_IN}" "${CONFIG_FILE_OUT}"
        INSTALL_DESTINATION "${INSTALL_DIR}")
write_basic_package_version_file("${VERSION_FILE}"
        COMPATIBILITY SameMajorVersion)

if(CALICODB_Install)
    install(TARGETS calicodb
            EXPORT "${TARGETS_NAME}"
            RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
            LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
            ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}")
    install(DIRECTORY "${CALICODB_INCLUDE_DIR}/calicodb"
            DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")

    install(EXPORT "${TARGETS_NAME}"
            NAMESPACE "${PROJECT_NAME}::"
            DESTINATION "${INSTALL_DIR}")
    install(FILES "${CONFIG_FILE_OUT}" "${VERSION_FILE}"
            DESTINATION "${INSTALL_DIR}")
endif()
