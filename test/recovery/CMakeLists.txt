build_test(fail fail.cpp)
build_test(recover recover.cpp)

# TODO: Need a bash script or something to automate this test. We can make it a dependency with configure_file(), so
#       if we change it, it gets automatically copied to the build tree.
#configure_file(
#    failure.sh
#    "${CMAKE_BINARY_DIR}/test/failure"
#    COPYONLY
#)