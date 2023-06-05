
set(BENCHMARK_ENABLE_TESTING Off CACHE INTERNAL "" FORCE)
set(BENCHMARK_ENABLE_EXCEPTIONS Off CACHE INTERNAL "" FORCE)
FetchContent_Declare(benchmark
        GIT_REPOSITORY https://github.com/google/benchmark.git
        GIT_TAG main)
FetchContent_MakeAvailable(benchmark)

function(build_benchmark LIB_NAME)
    set(NAME bench_${LIB_NAME})
    add_executable(${NAME} ${NAME}.cpp benchmark.h)
    target_link_libraries(${NAME} ${LIB_NAME} benchmark)
endfunction()

build_benchmark(calicodb)