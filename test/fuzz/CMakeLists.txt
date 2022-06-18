# Based off of the fuzzer build process in simdjson.
#
# See https://github.com/simdjson/simdjson/blob/master/fuzz/CMakeLists.txt for details.

option(CUB_BUILD_FUZZERS "Build the fuzz tests" OFF)
option(CUB_FUZZER_LINK_MAIN "Link each fuzzer with a main() for reproducing specific crashes" OFF)
set(CUB_FUZZER_LDFLAGS "" CACHE STRING "LDFLAGS for the fuzz targets")

if(CUB_BUILD_FUZZERS)
    add_library(fuzz fuzzers.h validators.cpp validators.h)
    target_link_libraries(fuzz PUBLIC "${CUB_NAME}" test_tools pthread)
    target_include_directories(fuzz PUBLIC "${CUB_TEST_DIR}/tools")
    target_include_directories(fuzz PUBLIC "${CUB_SOURCE_DIR}")

    if (CUB_FUZZER_LINK_MAIN)
        target_sources(fuzz INTERFACE main.cpp)
    else()
        target_link_libraries(fuzz INTERFACE "${CUB_FUZZER_LDFLAGS}")
    endif()

    add_executable(corpus_generator corpus.cpp fuzzers.h validators.cpp validators.h)
    target_link_libraries(corpus_generator PRIVATE "${CUB_NAME}" test_tools pthread)
    target_include_directories(corpus_generator PRIVATE "${CUB_TEST_DIR}/tools")
    target_include_directories(corpus_generator PRIVATE "${CUB_SOURCE_DIR}")
    target_compile_options(corpus_generator PRIVATE "${CUB_WARNINGS}")

    add_custom_target(all_fuzzers)

    function(build_fuzzer NAME)
        add_executable("${NAME}" "${NAME}.cpp")
        target_link_libraries(${NAME} PRIVATE fuzz)
        target_compile_options("${NAME}" PRIVATE "${CUB_WARNINGS}")
        add_dependencies(all_fuzzers ${NAME})
        add_test(NAME "${NAME}" COMMAND "${NAME}" -max_total_time=20)
    endfunction()

    build_fuzzer(fuzz_mem_ops)
    build_fuzzer(fuzz_ops)
    build_fuzzer(fuzz_wal_reader)
endif()