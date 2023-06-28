
option(CALICODB_FuzzerStandalone "Build with the standalone main() for reproducing fuzzer runs" On)

# Based off of the fuzzer CMakeLists.txt in fmt.
function(build_fuzzer NAME)
    set(TARGET ${NAME}_fuzzer)
    add_executable(${TARGET} ${TARGET}.cpp fuzzer.h)
    if(CALICODB_FuzzerStandalone)
        target_sources(${TARGET} PRIVATE main.cpp)
    endif()
    target_link_libraries(${TARGET} PRIVATE calicodb_utils)
    add_test(NAME ${TARGET} COMMAND ${TARGET} -seed 42 -max_total_time 30)
endfunction()

build_fuzzer(db)
