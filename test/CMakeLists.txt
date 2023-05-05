
option(CALICODB_BuildBenchmarks "Build the benchmarks" Off)
option(CALICODB_BuildFuzzers "Build the fuzz targets" Off)
option(CALICODB_BuildUnitTests "Build the unit tests" On)

set(CALICODB_TOOLS_DIR "${PROJECT_SOURCE_DIR}/test/tools")
add_subdirectory(tools)

if(CALICODB_BuildFuzzers)
    add_subdirectory(fuzzers)
endif()

if(CALICODB_BuildUnitTests)
    add_subdirectory(unit_tests)
endif()

if(CALICODB_BuildBenchmarks)
    add_subdirectory(benchmarks)
endif()


#add_subdirectory(debug)

