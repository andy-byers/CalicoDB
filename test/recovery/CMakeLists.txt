#add_executable(fail fail.cpp)
#target_link_libraries(fail PRIVATE test_tools)
#
#add_executable(recover recover.cpp)
#target_link_libraries(recover PRIVATE test_tools)
#
#configure_file(run.sh "${CMAKE_BINARY_DIR}/test/recovery" COPYONLY)
#add_test(NAME test_recovery COMMAND bash run.sh -n 25 -r 1000 -s 123 -v -w 2)

# TODO: Unfortunately, the API redesign has left these tests invalid. The batch write procedure doesn't update the database
#       until apply() is called, so it's more difficult to simulate failure. Probably the best bet will be to use mocks
#       to test failures during batch writes or normal update operations.