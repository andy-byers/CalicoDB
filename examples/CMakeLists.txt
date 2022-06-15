
add_executable(usage usage.cpp)
target_link_libraries(usage PRIVATE ${CUB_NAME})

add_executable(example example.cpp)
target_link_libraries(example PRIVATE ${CUB_NAME})
