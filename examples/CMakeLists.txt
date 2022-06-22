
add_executable(usage usage.cpp)
target_link_libraries(usage PRIVATE ${CALICO_NAME})

add_executable(example example.cpp)
target_link_libraries(example PRIVATE ${CALICO_NAME})
