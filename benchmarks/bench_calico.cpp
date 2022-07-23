
#include <filesystem>
#include "bench.h"
#include "spdlog/fmt/fmt.h"
#include "calico/calico.h"

using namespace cco;
namespace fs = std::filesystem;

// state.range(0): "should_recreate": true if we should recreate the database in each call to SetUp(), false otherwise.
// state.range(1): "is_persistent": true if the database should be persistent, false if it should be in-memory.

class CalicoBenchmarks: public benchmark::Fixture {
public:
    static constexpr auto PATH = "/tmp/calico_benchmarks__";
    static constexpr Size BATCH_SIZE {5'000};

    CalicoBenchmarks()
    {
        std::error_code ignore;
        fs::remove_all(PATH, ignore);
    }

    ~CalicoBenchmarks() override
    {
        std::error_code ignore;
        fs::remove_all(PATH, ignore);
    }

    auto SetUp(const benchmark::State &state) -> void
    {
        // If the "is_persistent" flag is set, we only create the database once. If we try to recreate it each time, we lose our cache each time this
        // method is called. This makes the benchmarks run extremely slow since each page must be reread from disk. Also, the first few batches where
        // google benchmark runs a small number of iterations become badly skewed.
        const bool is_persistent = state.range(0);

        if (!db || !is_persistent) {
            // 4 MB cache with pages of size 32 KB.
            db = std::make_unique<cco::Database>(cco::Options {
                is_persistent ? PATH : "",
                0x8000,
                128,
                spdlog::level::off,
                0666,
                true,
            });

            const auto s = db->open();
            if (!s.is_ok()) {
                fmt::print("failed to open benchmark database\n");
                fmt::print("(reason) {}\n", s.what());
                std::exit(EXIT_FAILURE);
            }
        }
    }

    auto TearDown(const benchmark::State &state) -> void
    {
        const bool is_persistent = state.range(0);
        if (!is_persistent) {
            const auto s = db->close();
            if (!s.is_ok()) {
                fmt::print("failed close transient database\n");
                fmt::print("(reason) {}\n", s.what());
                std::exit(EXIT_FAILURE);
            }
            return;
        }
        //
//        while (db->info().record_count() > 10'000) {
//            const auto s = db->erase(db->find_minimum());
//            if (!s.is_ok()) {
//                fmt::print("failed to keep benchmark database from getting too large\n");
//                fmt::print("(reason) {}\n", s.what());
//                std::exit(EXIT_FAILURE);
//            }
//        }
    }

    auto next_key() -> cco::BytesView
    {
        return generator.generate(16);
    }

    auto next_value() -> cco::BytesView
    {
        return generator.generate(100);
    }

    cco::RandomGenerator generator;
    std::unique_ptr<cco::Database> db;
};

auto handle_error(benchmark::State &state, Status s)
{
    if (!s.is_ok())
        state.SkipWithError(s.what().c_str());
}

BENCHMARK_DEFINE_F(CalicoBenchmarks, RandomWrites)(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        const auto key = next_key();
        const auto value = next_value();
        auto s = Status::ok();
        auto t = s;
        state.ResumeTiming();

        s = db->insert(key, value);
        t = db->commit();

        state.PauseTiming();
        handle_error(state, s);
        handle_error(state, t);
        state.ResumeTiming();
    }
}

BENCHMARK_DEFINE_F(CalicoBenchmarks, RandomReads)(benchmark::State& state) {
    while (db->info().record_count() < 10'000)
        benchmark::DoNotOptimize(db->insert(next_key(), next_value()));

    for (auto _ : state) {
        state.PauseTiming();
        const auto key = next_key();
        Cursor c;
        state.ResumeTiming();

        c = db->find(key);

        state.PauseTiming();
        const auto s = c.status();
        if (!s.is_ok() && !s.is_not_found())
            state.SkipWithError(s.what().c_str());
        state.ResumeTiming();
    }
}

BENCHMARK_DEFINE_F(CalicoBenchmarks, SequentialWrites)(benchmark::State& state) {
    Index i {};

    for (auto _ : state) {
        state.PauseTiming();
        const auto backing = fmt::format("{:016d}", i++);
        const auto key = stob(backing);
        const auto value = next_value();
        auto s = Status::ok();
        auto t = s;
        state.ResumeTiming();

        s = db->insert(key, value);
        t = db->commit();

        state.PauseTiming();
        handle_error(state, s);
        handle_error(state, t);
        state.ResumeTiming();
    }
}

BENCHMARK_DEFINE_F(CalicoBenchmarks, ForwardIteration)(benchmark::State& state) {
    while (db->info().record_count() < 10'000)
        benchmark::DoNotOptimize(db->insert(next_key(), next_value()));

    Cursor c;

    for (auto _ : state) {
        state.PauseTiming();
        if (!c.is_valid()) c = db->find_minimum();
        handle_error(state, c.status());
        state.ResumeTiming();
        const auto key = c.key();
        const auto value = c.value();
        benchmark::DoNotOptimize(key);
        benchmark::DoNotOptimize(value);
        c++;
    }
}

BENCHMARK_DEFINE_F(CalicoBenchmarks, ReverseIteration)(benchmark::State& state) {
    while (db->info().record_count() < 10'000)
        benchmark::DoNotOptimize(db->insert(next_key(), next_value()));

    Cursor c;

    for (auto _ : state) {
        state.PauseTiming();
        if (!c.is_valid()) c = db->find_maximum();
        handle_error(state, c.status());
        state.ResumeTiming();
        const auto key = c.key();
        const auto value = c.value();
        benchmark::DoNotOptimize(key);
        benchmark::DoNotOptimize(value);
        c--;
    }
}

BENCHMARK_DEFINE_F(CalicoBenchmarks, RandomBatchWrites)(benchmark::State& state) {
    long i {};
    for (auto _ : state) {
        state.PauseTiming();
        const auto key = next_key();
        const auto value = next_value();
        const auto should_commit = ++i % BATCH_SIZE == 0;
        auto s = Status::ok();
        auto t = s;
        state.ResumeTiming();

        s = db->insert(key, value);
        if (should_commit)
            t = db->commit();

        state.PauseTiming();
        handle_error(state, s);
        handle_error(state, t);
        state.ResumeTiming();
    }
}

BENCHMARK_DEFINE_F(CalicoBenchmarks, SequentialBatchWrites)(benchmark::State& state) {
    long i {};

    for (auto _ : state) {
        state.PauseTiming();
        const auto backing = fmt::format("{:016d}", i++);
        const auto should_commit = i % BATCH_SIZE == 0;
        const auto key = stob(backing);
        const auto value = next_value();
        auto s = Status::ok();
        auto t = s;
        state.ResumeTiming();

        s = db->insert(key, value);
        if (should_commit)
            t = db->commit();

        state.PauseTiming();
        handle_error(state, s);
        handle_error(state, t);
        state.ResumeTiming();
    }
}

auto compute_ops_per_second(const std::vector<double>& v) -> double
{
    fmt::print("{} {}\n", v.size(), v[0]);
    return 1.0;
}

constexpr long IN_MEMORY {0};
constexpr long PERSISTENT {1};

BENCHMARK_REGISTER_F(CalicoBenchmarks, RandomWrites)
    ->ComputeStatistics("ops_per_second", compute_ops_per_second)
    ->Arg(PERSISTENT)
    ->Arg(IN_MEMORY);
BENCHMARK_REGISTER_F(CalicoBenchmarks, RandomReads)
    ->ComputeStatistics("ops_per_second", compute_ops_per_second)
    ->Arg(PERSISTENT)
    ->Arg(IN_MEMORY);
BENCHMARK_REGISTER_F(CalicoBenchmarks, SequentialWrites)
    ->ComputeStatistics("ops_per_second", compute_ops_per_second)
    ->Arg(PERSISTENT)
    ->Arg(IN_MEMORY);
BENCHMARK_REGISTER_F(CalicoBenchmarks, ForwardIteration)
    ->ComputeStatistics("ops_per_second", compute_ops_per_second)
    ->Arg(PERSISTENT)
    ->Arg(IN_MEMORY);
BENCHMARK_REGISTER_F(CalicoBenchmarks, ReverseIteration)
    ->ComputeStatistics("ops_per_second", compute_ops_per_second)
    ->Arg(PERSISTENT)
    ->Arg(IN_MEMORY);
BENCHMARK_REGISTER_F(CalicoBenchmarks, RandomBatchWrites)
    ->ComputeStatistics("ops_per_second", compute_ops_per_second)
    ->Arg(PERSISTENT)
    ->Arg(IN_MEMORY);
BENCHMARK_REGISTER_F(CalicoBenchmarks, SequentialBatchWrites)
    ->ComputeStatistics("ops_per_second", compute_ops_per_second)
    ->Arg(PERSISTENT)
    ->Arg(IN_MEMORY);
BENCHMARK_MAIN();














