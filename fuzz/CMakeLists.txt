
option(CALICO_FUZZER_STANDALONE "Build with the standalone main() for reproducing fuzzer runs" On)

# Based off of the fuzzer CMakeLists.txt in fmt.
function(build_fuzzer NAME)
    add_executable(${NAME} ${NAME}.cpp fuzzer.h)
    if(CALICO_FUZZER_STANDALONE)
        target_sources(${NAME} PRIVATE main.cpp)
    endif()
    target_link_libraries(${NAME} PRIVATE calico_tools)
endfunction()

build_fuzzer(db_fuzzer)
build_fuzzer(map_fuzzer)