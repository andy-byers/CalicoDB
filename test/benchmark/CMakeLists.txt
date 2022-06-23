add_executable(benchmark main.cpp)
target_link_libraries(benchmark ${CALICO_NAME} test_tools pthread)
target_include_directories(benchmark PRIVATE ${CALICO_TEST_DIR}/tools)

