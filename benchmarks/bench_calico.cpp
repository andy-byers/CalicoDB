
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
    static constexpr Size BATCH_SIZE {10'000};

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

    auto SetUp(benchmark::State &state) -> void override
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
                0644,
                state.range(1) != 0,
                spdlog::level::off,
            });

            const auto s = db->open();
            if (!s.is_ok()) {
                fmt::print("failed to open in-memory database\n");
                fmt::print("(reason) {}\n", s.what());
                std::exit(EXIT_FAILURE);
            }
        }
    }

    auto TearDown(const benchmark::State &state) -> void override
    {
        const bool is_persistent = state.range(0);
        if (!is_persistent) {
            const auto s = db->close();
            if (!s.is_ok()) {
                fmt::print("failed to close in-memory database\n");
                fmt::print("(reason) {}\n", s.what());
                std::exit(EXIT_FAILURE);
            }
            return;
        }
    }

    auto next_key() -> cco::BytesView
    {
        return generator.generate(16);
    }

    auto next_value() -> cco::BytesView
    {
        return generator.generate(100);
    }

    auto next_large_value() -> cco::BytesView
    {
        return generator.generate(100'000);
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
        state.ResumeTiming();

        s = db->insert(key, value);

        state.PauseTiming();
        handle_error(state, s);
        state.ResumeTiming();
    }
}

BENCHMARK_DEFINE_F(CalicoBenchmarks, RandomReads)(benchmark::State& state) {
    while (db->info().record_count() < 50'000)
        benchmark::DoNotOptimize(db->insert(next_key(), next_value()));

    for (auto _ : state) {
        state.PauseTiming();
        const auto key = next_key();
        state.ResumeTiming();

        auto c = db->find(key);

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
        state.ResumeTiming();

        s = db->insert(key, value);

        state.PauseTiming();
        handle_error(state, s);
        state.ResumeTiming();
    }
}

BENCHMARK_DEFINE_F(CalicoBenchmarks, ForwardIteration)(benchmark::State& state) {
    while (db->info().record_count() < 50'000)
        benchmark::DoNotOptimize(db->insert(next_key(), next_value()));

    auto c = db->find_minimum();

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
    while (db->info().record_count() < 50'000)
        benchmark::DoNotOptimize(db->insert(next_key(), next_value()));

    auto c = db->find_minimum();

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
    Batch batch;
    for (auto _ : state) {
        state.PauseTiming();
        const auto key = next_key();
        const auto value = next_value();
        const auto should_commit = ++i % BATCH_SIZE == 0;
        auto s = Status::ok();
        state.ResumeTiming();

        batch.insert(key, value);
        if (should_commit) {
            s = db->apply(batch);
            batch = Batch {};
        }

        state.PauseTiming();
        handle_error(state, s);
        state.ResumeTiming();
    }
}

BENCHMARK_DEFINE_F(CalicoBenchmarks, SequentialBatchWrites)(benchmark::State& state) {
    long i {};
    Batch batch;
    for (auto _ : state) {
        state.PauseTiming();
        const auto backing = fmt::format("{:016d}", i++);
        const auto should_commit = i % BATCH_SIZE == 0;
        const auto key = stob(backing);
        const auto value = next_value();
        auto s = Status::ok();
        state.ResumeTiming();

        batch.insert(key, value);
        if (should_commit) {
            s = db->apply(batch);
            batch = Batch {};
        }

        state.PauseTiming();
        handle_error(state, s);
        state.ResumeTiming();
    }
}

BENCHMARK_DEFINE_F(CalicoBenchmarks, RandomBatchWrites100K)(benchmark::State& state) {
    Index i {};
    Batch batch;
    for (auto _ : state) {
        state.PauseTiming();
        const auto key = next_key();
        const auto value = next_large_value();
        const auto should_commit = ++i % BATCH_SIZE == 0;
        auto s = Status::ok();
        state.ResumeTiming();

        batch.insert(key, value);
        if (should_commit) {
            s = db->apply(batch);
            batch = Batch {};
        }

        state.PauseTiming();
        handle_error(state, s);
        state.ResumeTiming();
    }
}

BENCHMARK_DEFINE_F(CalicoBenchmarks, RandomReads100K)(benchmark::State& state) {
    while (db->info().record_count() < 1'000)
        benchmark::DoNotOptimize(db->insert(next_key(), next_large_value()));

    for (auto _ : state) {
        state.PauseTiming();
        const auto key = next_key();
        state.ResumeTiming();

        auto c = db->find(key);
        benchmark::DoNotOptimize(c.key());

        // Will traverse the overflow chain to get the whole value.
        benchmark::DoNotOptimize(c.value());

        state.PauseTiming();
        const auto s = c.status();
        if (!s.is_ok() && !s.is_not_found())
            state.SkipWithError(s.what().c_str());
        state.ResumeTiming();
    }
}

constexpr long IN_MEMORY {0};
constexpr long PERSISTENT {1};

BENCHMARK_REGISTER_F(CalicoBenchmarks, RandomWrites)
    ->Args({PERSISTENT, true})
    ->Args({PERSISTENT, false})
    ->Args({IN_MEMORY, true})
    ->Args({IN_MEMORY, false});
BENCHMARK_REGISTER_F(CalicoBenchmarks, SequentialWrites)
    ->Args({PERSISTENT, true})
    ->Args({PERSISTENT, false})
    ->Args({IN_MEMORY, true})
    ->Args({IN_MEMORY, false});
BENCHMARK_REGISTER_F(CalicoBenchmarks, RandomBatchWrites)
    ->Args({PERSISTENT, true})
    ->Args({PERSISTENT, false})
    ->Args({IN_MEMORY, true})
    ->Args({IN_MEMORY, false});
BENCHMARK_REGISTER_F(CalicoBenchmarks, SequentialBatchWrites)
    ->Args({PERSISTENT, true})
    ->Args({PERSISTENT, false})
    ->Args({IN_MEMORY, true})
    ->Args({IN_MEMORY, false});
BENCHMARK_REGISTER_F(CalicoBenchmarks, RandomReads)
    ->Args({PERSISTENT, true})
    ->Args({PERSISTENT, false})
    ->Args({IN_MEMORY, true})
    ->Args({IN_MEMORY, false});
BENCHMARK_REGISTER_F(CalicoBenchmarks, ForwardIteration)
    ->Args({PERSISTENT, true})
    ->Args({PERSISTENT, false})
    ->Args({IN_MEMORY, true})
    ->Args({IN_MEMORY, false});
BENCHMARK_REGISTER_F(CalicoBenchmarks, ReverseIteration)
    ->Args({PERSISTENT, true})
    ->Args({PERSISTENT, false})
    ->Args({IN_MEMORY, true})
    ->Args({IN_MEMORY, false});
BENCHMARK_REGISTER_F(CalicoBenchmarks, RandomBatchWrites100K)
    ->Args({PERSISTENT, true})
    ->Args({PERSISTENT, false})
    ->Args({IN_MEMORY, true})
    ->Args({IN_MEMORY, false});
BENCHMARK_REGISTER_F(CalicoBenchmarks, RandomReads100K)
    ->Args({PERSISTENT, true})
    ->Args({PERSISTENT, false})
    ->Args({IN_MEMORY, true})
    ->Args({IN_MEMORY, false});
BENCHMARK_MAIN();














