// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "tools.h"
#include <benchmark/benchmark.h>
#include <calicodb/db.h>

enum AccessType : int64_t {
    kSequential,
    kRandom,
};

class Benchmark final
{
    static constexpr auto kFilename = "__bench_db__";
    static constexpr std::size_t kKeyLength {16};
    static constexpr std::size_t kNumRecords {10'000};
    static constexpr std::size_t kCheckpointInterval {1'000};

public:
    struct Parameters {
        std::size_t value_length;
    };

    explicit Benchmark(const Parameters &param = {.value_length = 100})
        : m_param {param}
    {
        m_options.page_size = 0x2000;
        m_options.cache_size = 4'194'304;
        CHECK_OK(calicodb::DB::open(m_options, kFilename, m_db));
    }

    ~Benchmark()
    {
        delete m_cursor;
        delete m_db;

        CHECK_OK(calicodb::DB::destroy(m_options, kFilename));
    }

    auto read(benchmark::State &state) -> void
    {
        state.PauseTiming();
        const auto key = calicodb::tools::integral_key<kKeyLength>(
            state.range(0) == kSequential
                ? m_counter++ % kNumRecords
                : m_random.Next(kNumRecords - 1));
        state.ResumeTiming();

        std::string value;
        CHECK_OK(m_db->get(key, &value));
    }

    auto write(benchmark::State &state) -> void
    {
        state.PauseTiming();
        const auto key = calicodb::tools::integral_key<kKeyLength>(
            state.range(0) == kSequential ? m_counter : m_random.Next(kNumRecords));
        const auto value = m_random.Generate(m_param.value_length);
        state.ResumeTiming();

        CHECK_OK(m_db->put(key, value));
        if (++m_counter % kCheckpointInterval == 0) {
            CHECK_OK(m_db->checkpoint());
        }
    }

    auto step_forward(benchmark::State &state) -> void
    {
        state.PauseTiming();
        if (!m_cursor->is_valid()) {
            m_cursor->seek_first();
        }
        state.ResumeTiming();

        use_cursor();
        m_cursor->next();
    }

    auto step_backward(benchmark::State &state) -> void
    {
        state.PauseTiming();
        if (!m_cursor->is_valid()) {
            m_cursor->seek_last();
        }
        state.ResumeTiming();

        use_cursor();
        m_cursor->previous();
    }

    auto seek(benchmark::State &state) -> void
    {
        state.PauseTiming();
        const auto key = calicodb::tools::integral_key<kKeyLength>(
            state.range(0) == kSequential ? m_counter++ % kNumRecords
                                          : m_random.Next(kNumRecords - 1));
        state.ResumeTiming();

        m_cursor->seek(key);
        use_cursor();
    }

    auto setup_for_reads() -> void
    {
        for (std::size_t i {}; i < kNumRecords; ++i) {
            CHECK_OK(m_db->put(
                calicodb::tools::integral_key<kKeyLength>(i),
                m_random.Generate(m_param.value_length)));
        }
        CHECK_OK(m_db->checkpoint());
        m_cursor = m_db->new_cursor();
    }

private:
    auto use_cursor() const -> void
    {
        CHECK_TRUE(m_cursor->is_valid());
        auto result_key = m_cursor->key();
        auto result_value = m_cursor->value();
        benchmark::DoNotOptimize(result_key);
        benchmark::DoNotOptimize(result_value);
        benchmark::ClobberMemory();
    }

    Parameters m_param;
    std::size_t m_counter {};
    calicodb::tools::RandomGenerator m_random {4'194'304};
    calicodb::Options m_options;
    calicodb::Cursor *m_cursor {};
    calicodb::DB *m_db {};
};

static auto BM_Writes(benchmark::State &state) -> void
{
    Benchmark bench;
    for (auto _ : state) {
        bench.write(state);
    }
}
BENCHMARK(BM_Writes)
    ->Arg(kSequential)
    ->Arg(kRandom);

static auto BM_Reads(benchmark::State &state) -> void
{
    Benchmark bench;
    bench.setup_for_reads();
    for (auto _ : state) {
        bench.read(state);
    }
}
BENCHMARK(BM_Reads)
    ->Arg(kSequential)
    ->Arg(kRandom);

static auto BM_IterateForward(benchmark::State &state) -> void
{
    Benchmark bench;
    bench.setup_for_reads();
    for (auto _ : state) {
        bench.step_forward(state);
    }
}
BENCHMARK(BM_IterateForward);

static auto BM_IterateBackward(benchmark::State &state) -> void
{
    Benchmark bench;
    bench.setup_for_reads();
    for (auto _ : state) {
        bench.step_backward(state);
    }
}
BENCHMARK(BM_IterateBackward);

static auto BM_Seek(benchmark::State &state) -> void
{
    Benchmark bench;
    bench.setup_for_reads();
    for (auto _ : state) {
        bench.seek(state);
    }
}
BENCHMARK(BM_Seek)
    ->Arg(kSequential)
    ->Arg(kRandom);

static auto BM_Writes100K(benchmark::State &state) -> void
{
    Benchmark bench {{.value_length = 100'000}};
    for (auto _ : state) {
        bench.write(state);
    }
}
BENCHMARK(BM_Writes100K)
    ->Arg(kSequential)
    ->Arg(kRandom);

static auto BM_Reads100K(benchmark::State &state) -> void
{
    Benchmark bench {{.value_length = 100'000}};
    bench.setup_for_reads();
    for (auto _ : state) {
        bench.read(state);
    }
}
BENCHMARK(BM_Reads100K)
    ->Arg(kSequential)
    ->Arg(kRandom);

BENCHMARK_MAIN();