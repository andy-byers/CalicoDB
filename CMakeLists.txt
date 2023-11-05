cmake_minimum_required(VERSION 3.14)
set(CMAKE_TRY_COMPILE_TARGET_TYPE "STATIC_LIBRARY")

project(calicodb
        LANGUAGES CXX
        VERSION 0.2.0)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED On)
set(CMAKE_CXX_EXTENSIONS Off)

set(MAIN_PROJECT Off)
if (CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
    set(MAIN_PROJECT On)
endif()

option(CALICODB_BuildFuzzers "Build the fuzz targets" Off)
option(CALICODB_BuildTests "Build the tests" ${MAIN_PROJECT})
option(CALICODB_Install "Install the CMake targets during the install step" ${MAIN_PROJECT})
option(CALICODB_WithASan "Build with ASan" Off)
option(CALICODB_WithUBSan "Build with UBSan" Off)
option(CALICODB_WithTSan "Build with TSan" Off)
option(CALICODB_CI "Must be set if this is a CI build" Off)

set(CALICODB_OPTIONS "")
set(CALICODB_WARNINGS "")
# Set compiler warnings for the library (from @lefticus/cpp-best-practices), and make sure
# that neither exceptions, nor RTTI, are enabled.
if(${CMAKE_CXX_COMPILER_ID} STREQUAL MSVC)
    message(FATAL_ERROR 'Visual Studio is not yet supported')
else() # Clang OR GNU
    string(REGEX REPLACE "-fexceptions" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    string(REGEX REPLACE "-frtti" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

    set(CALICODB_OPTIONS
            -fno-exceptions
            -fno-rtti)
    set(CALICODB_WARNINGS
            -Wall
            -Wextra
            -Werror
            -Wnon-virtual-dtor
            -Wold-style-cast
            -Wcast-align
            -Wunused
            -Woverloaded-virtual
            -Wpedantic
            -Wconversion
            -Wsign-conversion
            -Wnull-dereference
            -Wdouble-promotion
            -Wimplicit-fallthrough)

    if(${CMAKE_CXX_COMPILER_ID} STREQUAL GNU)
        set(CALICODB_WARNINGS
                ${CALICODB_WARNINGS}
                -Wmisleading-indentation
                -Wduplicated-cond
                -Wduplicated-branches
                -Wlogical-op)
    endif()
endif()

add_library(calicodb STATIC)
target_sources(calicodb
        PRIVATE src/bucket.cpp
                src/bucket_impl.cpp
                src/bucket_impl.h
                src/buffer.h
                src/bufmgr.cpp
                src/bufmgr.h
                src/config.cpp
                src/config_internal.h
                src/cursor.cpp
                src/cursor_impl.cpp
                src/cursor_impl.h
                src/db.cpp
                src/db_impl.cpp
                src/db_impl.h
                src/encoding.h
                src/env.cpp
                src/env_posix.cpp
                src/freelist.cpp
                src/freelist.h
                src/header.cpp
                src/header.h
                src/internal.h
                src/internal_string.h
                src/internal_vector.h
        src/json_dom.cpp
        src/json_sax.cpp
        src/json.h
                src/list.h
                src/logging.cpp
                src/logging.h
                src/mem.cpp
                src/mem.h
                src/node.cpp
                src/node.h
                src/page.h
                src/pager.cpp
                src/pager.h
                src/pointer_map.cpp
                src/pointer_map.h
                src/schema.cpp
                src/schema.h
                src/status.cpp
                src/status_internal.h
                src/temp.cpp
                src/temp.h
                src/tree.cpp
                src/tree.h
                src/tx.cpp
                src/tx_impl.cpp
                src/tx_impl.h
                src/unique_ptr.h
                src/utility.h
                src/wal.cpp
                src/wal_internal.h
                port/port.h
        $<$<VERSION_GREATER:CMAKE_VERSION,3.2>:PUBLIC>
            include/calicodb/bucket.h
            include/calicodb/config.h
            include/calicodb/cursor.h
            include/calicodb/db.h
            include/calicodb/env.h
            include/calicodb/options.h
            include/calicodb/slice.h
            include/calicodb/stats.h
            include/calicodb/status.h
            include/calicodb/tx.h
            include/calicodb/wal.h)
target_include_directories(calicodb
        PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
               $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        PRIVATE ${PROJECT_SOURCE_DIR}/port)
target_compile_options(calicodb
        PRIVATE ${CALICODB_OPTIONS}
                ${CALICODB_WARNINGS})
target_compile_definitions(calicodb
        PUBLIC CALICODB_VERSION_MAJOR=${PROJECT_VERSION_MAJOR}
               CALICODB_VERSION_MINOR=${PROJECT_VERSION_MINOR}
               CALICODB_VERSION_PATCH=${PROJECT_VERSION_PATCH})

if(WIN32)
    message(FATAL_ERROR 'Windows is not yet supported')
    target_compile_definitions(calicodb
            PRIVATE CALICODB_PLATFORM_WINDOWS=1)
else()
    target_sources(calicodb
            PRIVATE src/env_posix.cpp
                    port/port_posix.cpp
                    port/port_posix.h)
    target_compile_definitions(calicodb
            PRIVATE CALICODB_PLATFORM_POSIX=1)
endif()

if(CALICODB_WithASan)
    target_compile_options(calicodb PUBLIC -fsanitize=address)
    target_link_options(calicodb PUBLIC -fsanitize=address)
endif()

if(CALICODB_WithUBSan)
    # RTTI required for -fsanitize=vptr.
    target_compile_options(calicodb PUBLIC -fsanitize=undefined -frtti)
    target_link_options(calicodb PUBLIC -fsanitize=undefined)
endif()

if(CALICODB_WithTSan)
    target_compile_options(calicodb PUBLIC -fsanitize=thread)
    target_link_options(calicodb PUBLIC -fsanitize=thread)
endif()

if(CALICODB_BuildTests OR CALICODB_BuildFuzzers)
    add_subdirectory(utils)
endif()

if(CALICODB_BuildTests)
    target_compile_definitions(calicodb
            PRIVATE CALICODB_TEST)
    include(FetchContent)
    include(CTest)
    enable_testing()
    add_subdirectory(test)
endif()

if(CALICODB_BuildFuzzers)
    add_subdirectory(fuzzers)
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
    install(DIRECTORY "${PROJECT_SOURCE_DIR}/include/calicodb"
            DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")

    install(EXPORT "${TARGETS_NAME}"
            NAMESPACE "${PROJECT_NAME}::"
            DESTINATION "${INSTALL_DIR}")
    install(FILES "${CONFIG_FILE_OUT}" "${VERSION_FILE}"
            DESTINATION "${INSTALL_DIR}")
endif()