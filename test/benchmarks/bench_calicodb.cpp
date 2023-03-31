// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "logging.h"
#include "tools.h"
#include <benchmark/benchmark.h>

enum AccessType : int64_t {
    kSequential,
    kRandom,
};

static auto access_type_name(int64_t type) -> std::string
{
    if (type == kSequential) {
        return "Sequential";
    } else if (type == kRandom) {
        return "Random";
    }
    return "Unknown";
}

struct Parameters {
    std::size_t value_length = 100;
    std::size_t commit_interval = 1;
};

class Benchmark final
{
    static constexpr auto kFilename = "__bench_db__";
    static constexpr std::size_t kKeyLength = 16;
    static constexpr std::size_t kNumRecords = 10'000;

public:
    explicit Benchmark(const Parameters &param = {})
        : m_param {param}
    {
        m_options.env = calicodb::Env::default_env();
        m_options.page_size = 1 << 15;
        m_options.cache_size = 4'194'304;
        CHECK_OK(calicodb::DB::open(m_options, kFilename, m_db));
    }

    ~Benchmark()
    {
        delete m_cursor;
        delete m_db;

        CHECK_OK(calicodb::DB::destroy(m_options, kFilename));

        delete m_options.env;
    }

    auto exists(benchmark::State &state) -> void
    {
        state.PauseTiming();
        const auto key = next_key(state.range(0) == kSequential, true);
        state.ResumeTiming();

        CHECK_OK(m_db->get(key, nullptr));

        increment_counters();
    }

    auto read(benchmark::State &state) -> void
    {
        state.PauseTiming();
        const auto key = next_key(state.range(0) == kSequential, true);
        state.ResumeTiming();

        std::string value;
        CHECK_OK(m_db->get(key, &value));

        increment_counters();
    }

    auto write(benchmark::State &state) -> void
    {
        state.PauseTiming();
        const auto key = next_key(state.range(0) == kSequential, state.range(1));
        const auto value = m_random.Generate(m_param.value_length);
        state.ResumeTiming();

        CHECK_OK(m_db->put(key, value));
        maybe_commit();
        increment_counters();
    }

    auto read_write(benchmark::State &state) -> void
    {
        state.PauseTiming();
        const auto is_read = m_counters[1] < state.range(1);
        const auto key = next_key(state.range(0) == kSequential, is_read);
        const auto value = m_random.Generate(m_param.value_length);
        m_counters[1] %= state.range(1);
        state.ResumeTiming();

        if (is_read) {
            std::string result;
            CHECK_OK(m_db->get(key, &result));
            benchmark::DoNotOptimize(result);
        } else {
            CHECK_OK(m_db->put(key, value));
            maybe_commit();
        }
        increment_counters();
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
        const auto key = next_key(state.range(0) == kSequential, true);
        state.ResumeTiming();

        m_cursor->seek(key);
        use_cursor();
        increment_counters();
    }

    auto add_initial_records() -> void
    {
        for (std::size_t i = 0; i < kNumRecords; ++i) {
            CHECK_OK(m_db->put(
                calicodb::tools::integral_key<kKeyLength>(i),
                m_random.Generate(m_param.value_length)));
        }
        CHECK_OK(m_db->commit());
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

    auto maybe_commit() -> void
    {
        if (m_counters[0] % m_param.commit_interval == m_param.commit_interval - 1) {
            CHECK_OK(m_db->commit());
        }
    }

    auto increment_counters() -> void
    {
        for (auto &counter : m_counters) {
            ++counter;
        }
    }

    [[nodiscard]] auto next_key(bool is_sequential, bool limit_key_range) -> std::string
    {
        auto m = 1'000'000'000'000;
        auto n = m_counters[0];
        if (limit_key_range) {
            m = kNumRecords - 1;
            n %= kNumRecords;
        }
        return calicodb::tools::integral_key<kKeyLength>(
            is_sequential ? n : m_random.Next(m));
    }

    Parameters m_param;
    std::size_t m_counters[2] {};
    calicodb::tools::RandomGenerator m_random {4'194'304};
    calicodb::Options m_options;
    calicodb::Cursor *m_cursor = nullptr;
    calicodb::DB *m_db = nullptr;
};

static auto set_modification_benchmark_label(benchmark::State &state)
{
    state.SetLabel(
        std::string(state.range(1) ? "Over" : "") +
        "Write" +
        access_type_name(state.range(0)) +
        (state.range(2) == 1 ? "" : "Batch"));
}

static auto BM_Write(benchmark::State &state) -> void
{
    set_modification_benchmark_label(state);

    Parameters param;
    param.commit_interval = state.range(2);

    Benchmark bench(param);
    for (auto _ : state) {
        bench.write(state);
    }
}
BENCHMARK(BM_Write)
    ->Args({kSequential, 0, 1})
    ->Args({kRandom, 0, 1})
    ->Args({kSequential, 0, 1'000})
    ->Args({kRandom, 0, 1'000});

static auto BM_Overwrite(benchmark::State &state) -> void
{
    set_modification_benchmark_label(state);

    Parameters param;
    param.commit_interval = state.range(2);

    Benchmark bench(param);
    bench.add_initial_records();
    for (auto _ : state) {
        bench.write(state);
    }
}
BENCHMARK(BM_Overwrite)
    ->Args({kSequential, 1, 1})
    ->Args({kRandom, 1, 1})
    ->Args({kSequential, 1, 1'000})
    ->Args({kRandom, 1, 1'000});

static auto BM_Exists(benchmark::State &state) -> void
{
    state.SetLabel("Exists" + access_type_name(state.range(0)));

    Benchmark bench;
    bench.add_initial_records();
    for (auto _ : state) {
        bench.exists(state);
    }
}
BENCHMARK(BM_Exists)
    ->Arg(kSequential)
    ->Arg(kRandom);

static auto BM_Read(benchmark::State &state) -> void
{
    state.SetLabel("Read" + access_type_name(state.range(0)));

    Benchmark bench;
    bench.add_initial_records();
    for (auto _ : state) {
        bench.read(state);
    }
}
BENCHMARK(BM_Read)
    ->Arg(kSequential)
    ->Arg(kRandom);

static auto BM_ReadWrite(benchmark::State &state) -> void
{
    const auto label = "ReadWrite" + access_type_name(state.range(0)) + "_1:";
    state.SetLabel(label + calicodb::number_to_string(state.range(1)));

    Benchmark bench;
    bench.add_initial_records();
    for (auto _ : state) {
        bench.read_write(state);
    }
}
BENCHMARK(BM_ReadWrite)
    ->Args({kSequential, 1})
    ->Args({kRandom, 1})
    ->Args({kSequential, 2})
    ->Args({kRandom, 2})
    ->Args({kSequential, 8})
    ->Args({kRandom, 8});

static auto BM_IterateForward(benchmark::State &state) -> void
{
    Benchmark bench;
    bench.add_initial_records();
    for (auto _ : state) {
        bench.step_forward(state);
    }
}
BENCHMARK(BM_IterateForward);

static auto BM_IterateBackward(benchmark::State &state) -> void
{
    Benchmark bench;
    bench.add_initial_records();
    for (auto _ : state) {
        bench.step_backward(state);
    }
}
BENCHMARK(BM_IterateBackward);

static auto BM_Seek(benchmark::State &state) -> void
{
    state.SetLabel("Seek" + access_type_name(state.range(0)));

    Benchmark bench;
    bench.add_initial_records();
    for (auto _ : state) {
        bench.seek(state);
    }
}
BENCHMARK(BM_Seek)
    ->Arg(kSequential)
    ->Arg(kRandom);

static auto BM_Write100K(benchmark::State &state) -> void
{
    state.SetLabel("Write" + access_type_name(state.range(0)) + "100K");

    Benchmark bench {{.value_length = 100'000}};
    for (auto _ : state) {
        bench.write(state);
    }
}
BENCHMARK(BM_Write100K)
    ->Args({kSequential, false})
    ->Args({kRandom, false});

static auto BM_Read100K(benchmark::State &state) -> void
{
    state.SetLabel("Read" + access_type_name(state.range(0)) + "100K");

    Benchmark bench {{.value_length = 100'000}};
    bench.add_initial_records();
    for (auto _ : state) {
        bench.read(state);
    }
}
BENCHMARK(BM_Read100K)
    ->Arg(kSequential)
    ->Arg(kRandom);

static auto BM_Exists100K(benchmark::State &state) -> void
{
    state.SetLabel("Exists" + access_type_name(state.range(0)) + "100K");

    Benchmark bench {{.value_length = 100'000}};
    bench.add_initial_records();
    for (auto _ : state) {
        bench.exists(state);
    }
}
BENCHMARK(BM_Exists100K)
    ->Arg(kSequential)
    ->Arg(kRandom);

BENCHMARK_MAIN();