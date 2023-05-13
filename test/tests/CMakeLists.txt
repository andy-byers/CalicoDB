
function(build_test NAME)
    add_executable("${NAME}"
            "${NAME}.cpp" common.cpp common.h test.cpp)
    target_link_libraries("${NAME}"
            PRIVATE calicodb_tools gtest)
    add_test(NAME "${NAME}"
            COMMAND "${NAME}")
endfunction()

#build_test(concurrency)
build_test(crashes)
build_test(operations)

add_executable(debug debug.cpp common.cpp common.h)
target_link_libraries(debug
        PRIVATE calicodb_tools gtest)
