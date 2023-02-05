add_executable(fail fail.cpp)
target_link_libraries(fail
        PRIVATE calico_tools)
target_include_directories(fail
        PRIVATE "${CALICO_SOURCE_DIR}")

add_executable(recover recover.cpp)
target_link_libraries(recover
        PRIVATE calico_tools)
target_include_directories(recover
        PRIVATE "${CALICO_SOURCE_DIR}")

configure_file(run.sh
        "${CMAKE_BINARY_DIR}/test/recovery" COPYONLY)
add_test(NAME test_recovery
        COMMAND bash run.sh -n 8 -r 1000 -s 0 -v -w 0.15)
