set(CALICO_HEADERS
    "${CALICO_INCLUDE_DIR}/calico/calico.h"
    "${CALICO_INCLUDE_DIR}/calico/common.h"
    "${CALICO_INCLUDE_DIR}/calico/cursor.h"
    "${CALICO_INCLUDE_DIR}/calico/database.h"
    "${CALICO_INCLUDE_DIR}/calico/options.h"
    "${CALICO_INCLUDE_DIR}/calico/slice.h"
    "${CALICO_INCLUDE_DIR}/calico/statistics.h"
    "${CALICO_INCLUDE_DIR}/calico/status.h"
    "${CALICO_INCLUDE_DIR}/calico/storage.h"
    "${CALICO_INCLUDE_DIR}/calico/transaction.h")
file(GLOB_RECURSE CALICO_SOURCES "*/*.cpp" "*/*.h")
list(APPEND CALICO_SOURCES "${CALICO_HEADERS}")

# @TartanLlama/expected source code.
set(TL_EXPECTED_DIR "${CALICO_THIRD_PARTY_DIR}/tl")
list(APPEND CALICO_SOURCES "${TL_EXPECTED_DIR}/expected.hpp")

# TODO: Get Calico DB to compile without exceptions, using "-fno-exceptions".

# Only use RTTI if we are not in release mode. We need it for printing the tree, but that code is only compiled when
# NDEBUG is set to 0.
set(CALICO_FLAGS "${CALICO_WARNINGS}")
list(APPEND CALICO_FLAGS -fno-exceptions)
string(TOLOWER ${CMAKE_BUILD_TYPE} BUILD_TYPE)
if(BUILD_TYPE MATCHES "^release")
    message("Adding -fno-rtti flag")
    list(APPEND CALICO_FLAGS -fno-rtti)
endif()

add_library(calico
    STATIC "${CALICO_SOURCES}")
target_include_directories(calico
    PUBLIC  $<BUILD_INTERFACE:${CALICO_INCLUDE_DIR}>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE "${CALICO_SOURCE_DIR}" "${TL_EXPECTED_DIR}")
target_include_directories(calico
    PUBLIC $<BUILD_INTERFACE:${TL_EXPECTED_DIR}>
           $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_compile_options(calico
    PRIVATE "${CALICO_FLAGS}")
target_compile_definitions(calico
    PUBLIC  CALICO_VERSION_MAJOR=${PROJECT_VERSION_MAJOR}
            CALICO_VERSION_MINOR=${PROJECT_VERSION_MINOR}
            CALICO_VERSION_PATCH=${PROJECT_VERSION_PATCH})
target_link_libraries(calico
    PRIVATE pthread spdlog)