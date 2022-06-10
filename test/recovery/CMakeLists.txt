build_test(fail fail.cpp)
build_test(recover recover.cpp)

configure_file(
    run.sh
    "${CMAKE_BINARY_DIR}/test/recovery"
    COPYONLY
)
add_test(NAME test_recovery COMMAND bash run.sh -n 25 -r 1000 -s 123 -v -w 2)
