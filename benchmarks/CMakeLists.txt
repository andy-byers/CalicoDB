
set(BENCHMARK_ENABLE_TESTING Off CACHE INTERNAL "")
set(BENCHMARK_ENABLE_EXCEPTIONS Off CACHE INTERNAL "")
set(BENCHMARK_ENABLE_GTEST_TESTS Off CACHE INTERNAL "")
set(BENCHMARK_USE_BUNDLED_GTEST Off CACHE INTERNAL "")
FetchContent_Declare(benchmark
    GIT_REPOSITORY https://github.com/google/benchmark.git
    GIT_TAG v1.7.1)
FetchContent_MakeAvailable(benchmark)

add_executable(bench_calico bench_calico.cpp)
target_link_libraries(bench_calico PRIVATE calico benchmark)
