
add_executable(usage usage.cpp)
target_link_libraries(usage PRIVATE ${CALICO_NAME})

add_executable(parts parts.cpp)
target_link_libraries(parts PRIVATE ${CALICO_NAME})
