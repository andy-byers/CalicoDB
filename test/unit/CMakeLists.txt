
add_executable(
    unit
    unit.cpp
    test_cache.cpp
    test_db.cpp
    test_file.cpp
    test_page.cpp
    test_pool.cpp
    test_tree.cpp
    test_utils.cpp
    test_wal.cpp
    ${TEST_TOOLS}
)

target_link_libraries(unit pthread ${CUB_NAME} gmock gtest)
target_include_directories(unit PRIVATE "${PROJECT_SOURCE_DIR}/test/tools")
target_include_directories(unit PRIVATE "${CUB_SOURCE_DIR}")
target_include_directories(unit PUBLIC "${CUB_INCLUDE_DIR}")