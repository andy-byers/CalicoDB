
add_executable(
    fuzz
    fuzz.cpp
    ${TEST_TOOLS}
)

target_compile_options(fuzz PRIVATE "-fsanitize=fuzzer")
target_link_libraries(fuzz pthread ${CUB_NAME})
target_include_directories(fuzz PRIVATE "${CUB_SOURCE_DIR}")
target_include_directories(fuzz PUBLIC "${CUB_INCLUDE_DIR}")