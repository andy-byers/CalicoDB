
option(CALICO_BuildFuzzers "Build the fuzz targets" Off)
option(CALICO_BuildUnitTests "Build the unit tests" On)

add_subdirectory(tools)

if(CALICO_BuildFuzzers)
    add_subdirectory(fuzzers)
endif()

if(CALICO_BuildUnitTests)
    add_subdirectory(unit_tests)
endif()