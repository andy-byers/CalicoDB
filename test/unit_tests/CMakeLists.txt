
set(gtest_force_shared_crt On CACHE BOOL "" FORCE)
set(INSTALL_GTEST Off CACHE INTERNAL "" FORCE)
set(INSTALL_GMOCK Off CACHE INTERNAL "" FORCE)
set(BUILD_GMOCK Off CACHE INTERNAL "" FORCE)
FetchContent_Declare(googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG release-1.11.0)
FetchContent_MakeAvailable(googletest)
include(GoogleTest)

add_executable(unit_tests
        test_impl.cpp
        test_pager.cpp
        test_recovery.cpp
        test_storage.cpp
        test_tree.cpp
        test_utils.cpp
        test_wal.cpp
        unit_tests.cpp
        unit_tests.h)
target_include_directories(unit_tests
        PRIVATE "${CALICO_SOURCE_DIR}")
target_link_libraries(unit_tests
        PUBLIC calico_tools gtest_main)
add_test(NAME unit_tests
        COMMAND unit_tests)

