
build_test(test_rw test_rw.cpp)
add_test(NAME test_rw COMMAND test_rw)

build_test(test_logging test_logging.cpp)
add_test(NAME test_logging COMMAND test_logging)
