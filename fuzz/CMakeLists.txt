
add_executable(db_fuzzer db_fuzzer.cpp fuzzer.h)
target_link_libraries(db_fuzzer PRIVATE calico)
add_test(NAME db_fuzzer COMMAND db_fuzzer -runs 25)

# TODO: Need to extract the "test tools" into a separate module so they can be used here and possibly in the benchmarks.
#if(CALICO_BUILD_TESTS)
#    add_executable(map_fuzzer map_fuzzer.cpp fuzzer.h ${TEST_TOOLS_FILES})
#    target_link_libraries(map_fuzzer PUBLIC calico pthread spdlog)
#    add_test(NAME map_fuzzer COMMAND map_fuzzer -runs 25)
#endif()