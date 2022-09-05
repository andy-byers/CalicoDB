
add_executable(api api.cpp)
target_link_libraries(api PRIVATE ${CALICO_NAME} spdlog)
add_test(NAME api COMMAND api)