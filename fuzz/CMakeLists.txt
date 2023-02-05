
add_executable(db_fuzzer
        db_fuzzer.cpp fuzzer.h)
target_link_libraries(db_fuzzer
        PRIVATE calico_tools)
add_test(NAME db_fuzzer
        COMMAND db_fuzzer -runs 25)

add_executable(map_fuzzer
        map_fuzzer.cpp fuzzer.h)
target_link_libraries(map_fuzzer
        PUBLIC calico_tools)
add_test(NAME map_fuzzer
        COMMAND map_fuzzer -runs 25)
