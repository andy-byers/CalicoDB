
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
#    list(APPEND CALICODB_OPTIONS -Wformat=2) TODO: Attribute to suppress warning from posix_env.cpp.
    list(APPEND CALICODB_OPTIONS -Wdouble-promotion)

    if(COMPILER_VENDOR STREQUAL GNU)
        list(APPEND CALICODB_OPTIONS -Wconversion)
        list(APPEND CALICODB_OPTIONS -Wsign-conversion)
        list(APPEND CALICODB_OPTIONS -Wduplicated-cond)
        list(APPEND CALICODB_OPTIONS -Wduplicated-branches)
        list(APPEND CALICODB_OPTIONS -Wlogical-op)
#        list(APPEND CALICODB_OPTIONS -Wnull-dereference)
#        list(APPEND CALICODB_OPTIONS -Wuseless-cast) # TODO: Problems on Apple Clang
    else()
        list(APPEND CALICODB_OPTIONS -Wimplicit-fallthrough)
    endif()
endif()

file(GLOB_RECURSE CALICODB_SOURCES "*.cpp" "*.h")
add_library(calicodb
        STATIC "${CALICODB_INCLUDE_DIR}/calicodb/calicodb.h"
               "${CALICODB_INCLUDE_DIR}/calicodb/cursor.h"
               "${CALICODB_INCLUDE_DIR}/calicodb/db.h"
               "${CALICODB_INCLUDE_DIR}/calicodb/env.h"
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
        PUBLIC  CALICODB_VERSION_MAJOR=${PROJECT_VERSION_MAJOR}
                CALICODB_VERSION_MINOR=${PROJECT_VERSION_MINOR}
                CALICODB_VERSION_PATCH=${PROJECT_VERSION_PATCH})
if(${CALICODB_BuildTests})
    target_compile_definitions(calicodb
            PRIVATE CALICODB_BUILD_TESTS)
endif()