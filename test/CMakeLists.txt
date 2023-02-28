
option(CALICODB_BuildFuzzers "Build the fuzz targets" Off)
option(CALICODB_BuildUnitTests "Build the unit tests" On)

add_subdirectory(tools)

if(CALICODB_BuildFuzzers)
    add_subdirectory(fuzzers)
endif()

if(CALICODB_BuildUnitTests)
    add_subdirectory(unit_tests)
endif()