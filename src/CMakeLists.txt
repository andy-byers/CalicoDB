set(CALICO_HEADERS
        "${CALICO_INCLUDE_DIR}/calico/calico.h"
        "${CALICO_INCLUDE_DIR}/calico/common.h"
        "${CALICO_INCLUDE_DIR}/calico/cursor.h"
        "${CALICO_INCLUDE_DIR}/calico/database.h"
        "${CALICO_INCLUDE_DIR}/calico/slice.h"
        "${CALICO_INCLUDE_DIR}/calico/status.h"
        "${CALICO_INCLUDE_DIR}/calico/storage.h")
file(GLOB_RECURSE CALICO_SOURCES "*/*.cpp" "*/*.h")
list(APPEND CALICO_SOURCES "${CALICO_HEADERS}")

set(CALICO_OPTIONS -fno-exceptions)
string(TOLOWER ${CMAKE_BUILD_TYPE} BUILD_TYPE)
if(NOT CALICO_BuildTests)
    list(APPEND CALICO_OPTIONS -fno-rtti)
endif()

# All applicable warnings from @lefticus/cpp-best-practices.
set(COMPILER_VENDOR ${CMAKE_CXX_COMPILER_ID})
if(COMPILER_VENDOR STREQUAL MSVC)
    message(FATAL_ERROR 'Visual Studio is not yet supported')
else()
    list(APPEND CALICO_OPTIONS -Werror)
    list(APPEND CALICO_OPTIONS -Wall)
    list(APPEND CALICO_OPTIONS -Wextra)
    list(APPEND CALICO_OPTIONS -Wshadow)
    list(APPEND CALICO_OPTIONS -Wnon-virtual-dtor)
    list(APPEND CALICO_OPTIONS -Wpedantic)
    list(APPEND CALICO_OPTIONS -Wold-style-cast)
    list(APPEND CALICO_OPTIONS -Wcast-align)
    list(APPEND CALICO_OPTIONS -Wunused)
    list(APPEND CALICO_OPTIONS -Woverloaded-virtual)
#    list(APPEND CALICO_OPTIONS -Wformat=2) TODO: Attribute to suppress warning from posix_storage.cpp.
    list(APPEND CALICO_OPTIONS -Wdouble-promotion)

    if(COMPILER_VENDOR STREQUAL GNU)
        list(APPEND CALICO_OPTIONS -Wconversion)
        list(APPEND CALICO_OPTIONS -Wsign-conversion)
        list(APPEND CALICO_OPTIONS -Wduplicated-cond)
        list(APPEND CALICO_OPTIONS -Wduplicated-branches)
        list(APPEND CALICO_OPTIONS -Wlogical-op)
        list(APPEND CALICO_OPTIONS -Wnull-dereference)
        list(APPEND CALICO_OPTIONS -Wuseless-cast)
    else()
        list(APPEND CALICO_OPTIONS -Wimplicit-fallthrough)
    endif()
endif()

add_library(calico
        STATIC "${CALICO_SOURCES}")
add_library(calico::calico
        ALIAS calico)
target_include_directories(calico
        PUBLIC  $<BUILD_INTERFACE:${CALICO_INCLUDE_DIR}>
                $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        PRIVATE "${CALICO_SOURCE_DIR}")
target_compile_options(calico
        PRIVATE "${CALICO_OPTIONS}")
target_compile_definitions(calico
        PUBLIC CALICO_VERSION_MAJOR=${PROJECT_VERSION_MAJOR}
               CALICO_VERSION_MINOR=${PROJECT_VERSION_MINOR}
               CALICO_VERSION_PATCH=${PROJECT_VERSION_PATCH}
               CALICO_BUILD_TESTS=${CALICO_BuildTests})