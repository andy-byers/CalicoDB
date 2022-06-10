build_test(fail fail.cpp)
build_test(recover recover.cpp)

configure_file(
    run.sh
    "${CMAKE_BINARY_DIR}/test/recovery"
    COPYONLY
)