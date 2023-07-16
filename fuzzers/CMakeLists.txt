
option(CALICODB_FuzzerStandalone "Build with the standalone main() for reproducing fuzzer runs" On)

# Based off of the fuzzer CMakeLists.txt in fmt.
function(build_fuzzer NAME)
    set(TARGET ${NAME}_fuzzer)
    add_executable(${TARGET} ${TARGET}.cpp fuzzer.h)
    if(CALICODB_FuzzerStandalone)
        target_sources(${TARGET} PRIVATE main.cpp)
    endif()
    target_link_libraries(${TARGET} PRIVATE calicodb_utils)
endfunction()

build_fuzzer(db_bucket)
build_fuzzer(db_model)
