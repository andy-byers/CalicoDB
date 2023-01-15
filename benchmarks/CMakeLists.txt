
set(BENCHMARK_ENABLE_TESTING Off CACHE INTERNAL "")
set(BENCHMARK_ENABLE_EXCEPTIONS Off CACHE INTERNAL "")
set(BENCHMARK_ENABLE_GTEST_TESTS Off CACHE INTERNAL "")
set(BENCHMARK_USE_BUNDLED_GTEST Off CACHE INTERNAL "")
FetchContent_Declare(benchmark
    GIT_REPOSITORY https://github.com/google/benchmark.git
    GIT_TAG v1.7.1)
FetchContent_MakeAvailable(benchmark)

function(build_benchmark LIBRARY_NAME)
    set(BENCHMARK_NAME "bench_${LIBRARY_NAME}")
    add_executable("${BENCHMARK_NAME}"
            "${BENCHMARK_NAME}.cpp" bench.h)

    # Make sure the benchmark links with calico, as well as the database library being benchmarked.
    set(LINK "${LIBRARY_NAME}")
    if(NOT LIBRARY_NAME STREQUAL calico)
        list(APPEND LINK calico)
    endif()
    target_link_libraries("${BENCHMARK_NAME}"
        PRIVATE benchmark "${LINK}")
endfunction(build_benchmark)

build_benchmark(calico)

check_library_exists(sqlite3 sqlite3_open "" HAVE_SQLITE3)
if(HAVE_SQLITE3)
    build_benchmark(sqlite3)
endif()
