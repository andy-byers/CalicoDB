
add_executable(db_fuzzer db_fuzzer.cpp)
target_link_libraries(db_fuzzer PRIVATE ${CALICO_NAME})
add_test(NAME db_fuzzer COMMAND db_fuzzer)