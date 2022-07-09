
add_executable(test_rw test_rw.cpp)
target_link_libraries(test_rw PRIVATE test_tools gtest_main)
add_test(NAME test_rw COMMAND test_rw)

add_executable(test_logging test_logging.cpp)
target_link_libraries(test_logging PRIVATE test_tools gtest_main)
target_include_directories(test_logging PRIVATE ${ZLIB_SOURCE_DIR} ${ZLIB_BINARY_DIR})
add_test(NAME test_logging COMMAND test_logging)