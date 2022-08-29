
add_executable(api api.cpp)
target_link_libraries(api PRIVATE ${CALICO_NAME})
add_test(NAME api COMMAND api)