#add_library(fuzz fuzz.cpp fuzz.h)
#target_link_libraries(fuzz PUBLIC test_tools)
#
#if (CCO_FUZZER_LINK_MAIN)
#    target_sources(fuzz INTERFACE main.cpp)
#else()
#    target_compile_options(fuzz INTERFACE ${CCO_FUZZER_LDFLAGS})
#endif()
#
##    add_executable(corpus_generator corpus.cpp fuzzers.h validators.cpp validators.h)
##    target_link_libraries(corpus_generator PRIVATE "${CCO_NAME}" test_tools pthread)
##    target_include_directories(corpus_generator PRIVATE "${CCO_TEST_DIR}/tools")
##    target_include_directories(corpus_generator PRIVATE "${CCO_SOURCE_DIR}")
##    target_compile_options(corpus_generator PRIVATE "${CCO_WARNINGS}")
#
#add_custom_target(all_fuzzers)
#
#function(build_fuzzer NAME)
#    add_executable("${NAME}" "${NAME}.cpp")
#    target_link_libraries(${NAME} PRIVATE fuzz)
#    target_compile_options("${NAME}" PRIVATE "${CCO_WARNINGS}")
#    add_dependencies(all_fuzzers ${NAME})
##        add_test(NAME "${NAME}" COMMAND "${NAME}" -max_total_time=20)
#endfunction()
#
#build_fuzzer(fuzz_db_modify)
#build_fuzzer(fuzz_db_xact)
