
option(CALICO_FuzzerStandalone "Build with the standalone main() for reproducing fuzzer runs" On)

# Based off of the fuzzer CMakeLists.txt in fmt.
function(build_fuzzer NAME)
    add_executable(${NAME} ${NAME}.cpp fuzzer.h)
    if(CALICO_FuzzerStandalone)
        target_sources(${NAME} PRIVATE main.cpp)
    endif()
    target_link_libraries(${NAME} PRIVATE calico_tools)
    target_include_directories(${NAME} PRIVATE "${CALICO_SOURCE_DIR}")
endfunction()

build_fuzzer(ops_fuzzer)
build_fuzzer(map_fuzzer)