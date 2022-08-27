add_library(fuzz fuzz.cpp fuzz.h)
target_link_libraries(fuzz PUBLIC test_tools)

if (CALICO_FUZZER_LINK_MAIN)
    target_sources(fuzz INTERFACE main.cpp)
else()
    target_compile_options(fuzz INTERFACE ${CALICO_FUZZER_LDFLAGS})
endif()

add_custom_target(all_fuzzers)
#add_custom_target(corpus)

function(build_fuzzer NAME)
    add_executable("${NAME}" "${NAME}.cpp")
    target_link_libraries(${NAME} PRIVATE fuzz)
    target_compile_options("${NAME}" PRIVATE "${CALICO_WARNINGS}")
    add_dependencies(all_fuzzers ${NAME})
#        add_test(NAME "${NAME}" COMMAND "${NAME}" -max_total_time=20)
endfunction()

build_fuzzer(db_mods)
build_fuzzer(db_xact)

# TODO: Copy corpus to build directory as part of build, then uncomment the add_test() above to add to CI. Should run
#       the CI fuzz tests on the corpus. libFuzzer may not work on the OSX builds though, so we'll need to disable them
#       on that platform.