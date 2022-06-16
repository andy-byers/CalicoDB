add_executable(benchmark main.cpp)
target_link_libraries(benchmark ${CUB_NAME} test_tools pthread)
target_include_directories(benchmark PRIVATE ${CUB_TEST_DIR}/tools)

