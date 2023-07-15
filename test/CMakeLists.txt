
set(gtest_force_shared_crt On CACHE BOOL "" FORCE)
set(INSTALL_GTEST Off CACHE INTERNAL "" FORCE)
set(INSTALL_GMOCK Off CACHE INTERNAL "" FORCE)
set(BUILD_GMOCK Off CACHE INTERNAL "" FORCE)
FetchContent_Declare(googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG release-1.11.0)
FetchContent_MakeAvailable(googletest)
include(GoogleTest)

function(build_test NAME)
    add_executable(${NAME} ${NAME}.cpp test.cpp test.h)
    target_compile_options(${NAME} PRIVATE ${CALICODB_FLAGS})
    target_link_libraries(${NAME} calicodb_utils gtest_main)
    target_include_directories(${NAME} PUBLIC "${CALICODB_SOURCE_DIR}")
    add_test(NAME ${NAME} COMMAND ${NAME})
endfunction()

build_test(test_bounds)
build_test(test_concurrency)
build_test(test_crashes)
build_test(test_db)
build_test(test_env)
build_test(test_node)
build_test(test_pager)
build_test(test_tree)
build_test(test_utils)

add_subdirectory(doc_tests)

# CMake tests were adapted from @nlohmann/json.
add_subdirectory(cmake_tests/fetch_content)
add_subdirectory(cmake_tests/add_subdirectory)
