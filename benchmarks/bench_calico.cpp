#include <benchmark/benchmark.h>
#include <calico/calico.h>

static constexpr auto DB_PATH = "./__bench_calico";
static constexpr Calico::Options DB_OPTIONS {
    0x2000,
    0x200000,
};

template<std::size_t Length = 20>
auto make_key(Calico::Size key) -> std::string
{
    auto key_string = std::to_string(key);
    return std::string(Length - key_string.size(), '0') + key_string;
}

static void BM_SequentialWrites(benchmark::State& state)
{
    Calico::Database db;
    assert(db.open(DB_PATH, DB_OPTIONS).is_ok());
    auto xact = db.transaction();

    std::string value(100, ' ');
    Calico::Size i {};

    for (auto _ : state) {
        state.PauseTiming();
        auto key = make_key<6>(i++);
        state.ResumeTiming();
        assert(db.insert(key, value).is_ok());
    }
}
BENCHMARK(BM_SequentialWrites);

BENCHMARK_MAIN();