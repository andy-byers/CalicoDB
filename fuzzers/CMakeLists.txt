
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
build_fuzzer(db_format)
build_fuzzer(db_model)

#add_library(libsqlite
#        shell.c
#        sqlite3.c
#        sqlite3.h
#        sqlite3ext.h)
add_executable(sqlite_format_fuzzer sqlite_format_fuzzer.cpp)
target_link_libraries(sqlite_format_fuzzer ${CMAKE_CURRENT_SOURCE_DIR}/sqlite_asan)
