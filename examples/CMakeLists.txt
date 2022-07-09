
add_executable(usage usage.cpp)
target_link_libraries(usage PRIVATE ${CALICO_NAME})

add_executable(perf perf.cpp)
target_link_libraries(perf PRIVATE ${CALICO_NAME} test_tools)