
option(CALICODB_BuildBenchmarks "Build the benchmarks" Off)
option(CALICODB_BuildFuzzers "Build the fuzz targets" Off)
option(CALICODB_BuildCrashTests "Build the crash tests" On)
option(CALICODB_BuildDocTests "Build the documentation tests" On)
option(CALICODB_BuildUnitTests "Build the unit tests" On)

set(gtest_force_shared_crt On CACHE BOOL "" FORCE)
set(INSTALL_GTEST Off CACHE INTERNAL "" FORCE)
set(INSTALL_GMOCK Off CACHE INTERNAL "" FORCE)
set(BUILD_GMOCK Off CACHE INTERNAL "" FORCE)
FetchContent_Declare(googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG release-1.11.0)
FetchContent_MakeAvailable(googletest)
include(GoogleTest)

set(CALICODB_TOOLS_DIR "${PROJECT_SOURCE_DIR}/test/tools")
add_subdirectory(tools)

#if(CALICODB_BuildCrashTests)
#    add_subdirectory(crash_tests)
#endif()

if(CALICODB_BuildDocTests)
    add_subdirectory(doc_tests)
endif()

if(CALICODB_BuildUnitTests)
    add_subdirectory(unit_tests)
endif()

if(CALICODB_BuildFuzzers)
    add_subdirectory(fuzzers)
endif()

if(CALICODB_BuildBenchmarks)
    add_subdirectory(benchmarks)
endif()
