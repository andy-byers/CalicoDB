
set(gtest_force_shared_crt On CACHE BOOL "" FORCE)
set(INSTALL_GTEST Off CACHE INTERNAL "" FORCE)
set(INSTALL_GMOCK Off CACHE INTERNAL "" FORCE)
set(BUILD_GMOCK Off CACHE INTERNAL "" FORCE)
FetchContent_Declare(googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG release-1.11.0)
FetchContent_MakeAvailable(googletest)
include(GoogleTest)

add_subdirectory(unit_tests)
add_subdirectory(recovery)
