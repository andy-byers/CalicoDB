
add_executable(bench_calico bench.h bench_calico.cpp)
target_link_libraries(bench_calico calico benchmark::benchmark)