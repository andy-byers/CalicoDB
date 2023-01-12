set(CALICO_HEADERS
    "${CALICO_INCLUDE_DIR}/calico/calico.h"
    "${CALICO_INCLUDE_DIR}/calico/common.h"
    "${CALICO_INCLUDE_DIR}/calico/cursor.h"
    "${CALICO_INCLUDE_DIR}/calico/database.h"
    "${CALICO_INCLUDE_DIR}/calico/info.h"
    "${CALICO_INCLUDE_DIR}/calico/options.h"
    "${CALICO_INCLUDE_DIR}/calico/platform.h"
    "${CALICO_INCLUDE_DIR}/calico/slice.h"
    "${CALICO_INCLUDE_DIR}/calico/status.h"
    "${CALICO_INCLUDE_DIR}/calico/storage.h"
    "${CALICO_INCLUDE_DIR}/calico/transaction.h")
set(CALICO_SOURCES '' ../benchmarks/bench_calico.cpp)
file(GLOB_RECURSE CALICO_SOURCES "./*/*.cpp" "./*/*.h")
list(APPEND CALICO_SOURCES "${CALICO_HEADERS}")


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
    PRIVATE expected pthread spdlog)
