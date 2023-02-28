
option(CALICODB_FuzzerStandalone "Build with the standalone main() for reproducing fuzzer runs" On)

# Based off of the fuzzer CMakeLists.txt in fmt.
function(build_fuzzer NAME)
    add_executable(${NAME} ${NAME}.cpp ${NAME}.h fuzzer.cpp fuzzer.h)
    if(CALICODB_FuzzerStandalone)
        target_sources(${NAME} PRIVATE main.cpp)
    endif()
    target_link_libraries(${NAME} PRIVATE calicodb_tools)
    target_include_directories(${NAME} PRIVATE "${CALICODB_SOURCE_DIR}")
endfunction()

build_fuzzer(ops_fuzzer)
build_fuzzer(map_fuzzer)