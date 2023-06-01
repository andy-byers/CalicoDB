
set(BENCHMARK_ENABLE_TESTING Off CACHE INTERNAL "" FORCE)
set(BENCHMARK_ENABLE_EXCEPTIONS Off CACHE INTERNAL "" FORCE)
FetchContent_Declare(benchmark
        GIT_REPOSITORY https://github.com/google/benchmark.git
        GIT_TAG main)
FetchContent_MakeAvailable(benchmark)

add_executable(bench_calicodb
        bench_calicodb.cpp)
target_link_libraries(bench_calicodb
        PRIVATE benchmark calicodb_tools)