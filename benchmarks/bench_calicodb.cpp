// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "benchmark.h"
#include "benchmark/benchmark.h"
#include "calicodb/cursor.h"
#include "calicodb/db.h"

// This simulates normal transaction behavior, where m_tx is destroyed and DB::new_tx() is called after
// each commit. This is what happens if the DB::view()/DB::update() API is used. It's much faster to keep
// the transaction object around and just call Tx::commit() and Tx::rollback() as needed, but this is
// bad for concurrency.
// NOTE: I'm also adding a checkpoint call right before the restart, to be run once every CHECKPOINT_SCALE
//       restarts.
#define RESTART_ON_COMMIT 1
#define CHECKPOINT_SCALE 100

#define CHECK_OK(expr)                                                 \
    do {                                                               \
        auto check_s = (expr);                                         \
        if (!check_s.is_ok()) {                                        \
            std::fprintf(stderr, "%s\n", check_s.to_string().c_str()); \
            std::abort();                                              \
        }                                                              \
    } while (0)

using namespace calicodb;
using namespace calicodb::benchmarks;

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
    bool sync = false;
    bool excl = false; // TODO
};

class Benchmark final
{
public:
    explicit Benchmark(const Parameters &param = {})
        : m_param(param),
          m_random(4 * 1'024 * 1'024)
    {
        (void)DB::destroy(m_options, kFilename);
        m_options.lock_mode = param.excl
                                  ? Options::kLockExclusive
                                  : Options::kLockNormal;
        m_options.sync_mode = param.sync
                                  ? Options::kSyncFull
                                  : Options::kSyncNormal;
        m_options.error_if_exists = true;
        CHECK_OK(DB::open(m_options, kFilename, m_db));
        CHECK_OK(m_db->update([](auto &tx) {
            // Make sure this bucket always exists for readers to open.
            return tx.create_bucket(BucketOptions(), "bench", nullptr);
        }));
    }

    ~Benchmark()
    {
        delete m_cursor;
        delete m_rd;
        delete m_wr;
        delete m_db;

        (void)DB::destroy(m_options, kFilename);
    }

    using InitOptions = unsigned;
    static constexpr InitOptions kPrefill = 1;
    static constexpr InitOptions kWriter = 2;
    static constexpr InitOptions kCursor = 4;
    auto init(InitOptions opt) -> void
    {
        if (opt & kPrefill) {
            CHECK_OK(m_db->update([this](auto &tx) {
                Bucket b;
                auto s = tx.open_bucket("bench", b);
                for (std::size_t i = 0; s.is_ok() && i < kNumRecords; ++i) {
                    s = tx.put(b, numeric_key<kKeyLength>(i),
                               m_random.Generate(m_param.value_length));
                }
                return s;
            }));
        }

        if (opt & kWriter) {
            CHECK_OK(m_db->new_tx(WriteTag(), m_wr));
        } else {
            CHECK_OK(m_db->new_tx(m_rd));
        }
        CHECK_OK(current_tx().open_bucket("bench", m_bucket));
        if (opt & kCursor) {
            m_cursor = current_tx().new_cursor(m_bucket);
        }
    }

    auto current_tx() const -> const Tx &
    {
        return m_rd ? *m_rd : *m_wr;
    }

    auto read(benchmark::State &state, std::string *out) -> void
    {
        state.PauseTiming();
        const auto key = next_key(state.range(0) == kSequential, true);
        if (out) {
            // Allocate new memory for the value each round.
            out->clear();
        }
        state.ResumeTiming();

        CHECK_OK(m_rd->get(m_bucket, key, out));
        benchmark::DoNotOptimize(out);

        increment_counters();
    }

    auto write(benchmark::State &state) -> void
    {
        state.PauseTiming();
        const auto key = next_key(state.range(0) == kSequential, state.range(1));
        const auto value = m_random.Generate(m_param.value_length);
        state.ResumeTiming();

        CHECK_OK(m_wr->put(m_bucket, key, value));
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
            CHECK_OK(m_wr->get(m_bucket, key, &result));
            benchmark::DoNotOptimize(result);
        } else {
            CHECK_OK(m_wr->put(m_bucket, key, value));
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

    auto vacuum(benchmark::State &state, std::size_t upper_size) -> void
    {
        state.PauseTiming();
        const auto lower_size = upper_size * state.range(0) / 10;
        assert(lower_size <= upper_size);
        for (std::size_t i = 0; i < upper_size; ++i) {
            CHECK_OK(m_wr->put(
                m_bucket,
                numeric_key<kKeyLength>(i),
                m_random.Generate(m_param.value_length)));
        }
        for (auto i = lower_size; i < upper_size; ++i) {
            CHECK_OK(m_wr->erase(
                m_bucket,
                numeric_key<kKeyLength>(i)));
        }
        state.ResumeTiming();

        CHECK_OK(m_wr->vacuum());
        CHECK_OK(m_wr->commit());
        restart_tx();
        increment_counters();
    }

private:
    auto use_cursor() const -> void
    {
        assert(m_cursor->is_valid());
        auto result_key = m_cursor->key();
        auto result_value = m_cursor->value();
        benchmark::DoNotOptimize(result_key);
        benchmark::DoNotOptimize(result_value);
        benchmark::ClobberMemory();
    }

    auto maybe_commit() -> void
    {
        const auto interval = m_param.commit_interval;
        if (m_counters[0] % interval + 1 == interval) {
            CHECK_OK(m_wr->commit());
#if RESTART_ON_COMMIT
            restart_tx();
#endif
        }
    }

    auto restart_tx() -> void
    {
        delete m_wr;

        const auto interval = m_param.commit_interval * CHECKPOINT_SCALE;
        if (m_counters[0] % interval + 1 == interval) {
            CHECK_OK(m_db->checkpoint(false));
        }

        CHECK_OK(m_db->new_tx(WriteTag(), m_wr));
        CHECK_OK(m_wr->open_bucket("bench", m_bucket));
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
        const auto key = is_sequential ? n : m_random.Next(m);

        char buffer[30];
        std::snprintf(
            buffer,
            sizeof(buffer),
            "%016lu",
            key);
        return {buffer};
    }

    static constexpr auto kFilename = "__bench_db__";
    static constexpr std::size_t kKeyLength = 16;
    static constexpr std::size_t kNumRecords = 10'000;
    const Tx *m_rd = nullptr;
    Tx *m_wr = nullptr;
    Bucket m_bucket;
    Parameters m_param;
    std::size_t m_counters[2] = {};
    benchmarks::RandomGenerator m_random;
    Options m_options;
    Cursor *m_cursor = nullptr;
    DB *m_db = nullptr;
};

static auto set_modification_benchmark_label(benchmark::State &state)
{
    state.SetLabel(
        (state.range(3) ? "Sync_" : "") +
        std::string(state.range(1) ? "Overwrite" : "Write") +
        access_type_name(state.range(0)) +
        (state.range(2) == 1 ? "" : "Batch"));
}

static auto BM_Write(benchmark::State &state) -> void
{
    set_modification_benchmark_label(state);

    Parameters param;
    param.commit_interval = state.range(2);
    param.sync = state.range(3);

    Benchmark bench(param);
    bench.init(Benchmark::kWriter);
    for (auto _ : state) {
        bench.write(state);
    }
}
BENCHMARK(BM_Write)
    ->Args({kSequential, false, 1, false})
    ->Args({kRandom, false, 1, false})
    ->Args({kSequential, false, 1'000, false})
    ->Args({kRandom, false, 1'000, false})
    ->Args({kSequential, false, 1, true})
    ->Args({kRandom, false, 1, true})
    ->Args({kSequential, false, 1'000, true})
    ->Args({kRandom, false, 1'000, true});

static auto BM_Overwrite(benchmark::State &state) -> void
{
    set_modification_benchmark_label(state);

    Parameters param;
    param.commit_interval = state.range(2);
    param.sync = state.range(3);

    Benchmark bench(param);
    bench.init(Benchmark::kWriter | Benchmark::kPrefill);
    for (auto _ : state) {
        bench.write(state);
    }
}
BENCHMARK(BM_Overwrite)
    ->Args({kSequential, true, 1, false})
    ->Args({kRandom, true, 1, false})
    ->Args({kSequential, true, 1'000, false})
    ->Args({kRandom, true, 1'000, false})
    ->Args({kSequential, true, 1, true})
    ->Args({kRandom, true, 1, true})
    ->Args({kSequential, true, 1'000, true})
    ->Args({kRandom, true, 1'000, true});

static auto BM_Vacuum(benchmark::State &state) -> void
{
    static constexpr std::size_t kUpperSize = 1'000;
    std::string label("Vacuum");
    if (state.range(0) == 1) {
        label.append("Few");
    } else if (state.range(0) == 5) {
        label.append("Half");
    } else if (state.range(0) == 10) {
        label.append("All");
    }
    state.SetLabel(label);

    Benchmark bench(Parameters{});
    bench.init(Benchmark::kWriter);
    for (auto _ : state) {
        bench.vacuum(state, kUpperSize);
    }
}
BENCHMARK(BM_Vacuum)
    ->Args({1})
    ->Args({5})
    ->Args({10});

static auto BM_Exists(benchmark::State &state) -> void
{
    state.SetLabel("Exists" + access_type_name(state.range(0)));

    Benchmark bench;
    bench.init(Benchmark::kPrefill);
    for (auto _ : state) {
        bench.read(state, nullptr);
    }
}
BENCHMARK(BM_Exists)
    ->Arg(kSequential)
    ->Arg(kRandom);

static auto BM_Read(benchmark::State &state) -> void
{
    state.SetLabel("Read" + access_type_name(state.range(0)));
    std::string value;

    Benchmark bench;
    bench.init(Benchmark::kPrefill);

    for (auto _ : state) {
        bench.read(state, &value);
        benchmark::DoNotOptimize(value);
    }
}
BENCHMARK(BM_Read)
    ->Arg(kSequential)
    ->Arg(kRandom);

static auto BM_ReadWrite(benchmark::State &state) -> void
{
    const auto label = "ReadWrite" + access_type_name(state.range(0)) + "_1:";
    state.SetLabel(label + std::to_string(state.range(1)));

    Benchmark bench;
    bench.init(Benchmark::kWriter | Benchmark::kPrefill);
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
    bench.init(Benchmark::kCursor | Benchmark::kPrefill);
    for (auto _ : state) {
        bench.step_forward(state);
    }
}
BENCHMARK(BM_IterateForward);

static auto BM_IterateBackward(benchmark::State &state) -> void
{
    Benchmark bench;
    bench.init(Benchmark::kCursor | Benchmark::kPrefill);
    for (auto _ : state) {
        bench.step_backward(state);
    }
}
BENCHMARK(BM_IterateBackward);

static auto BM_Seek(benchmark::State &state) -> void
{
    state.SetLabel("Seek" + access_type_name(state.range(0)));

    Benchmark bench;
    bench.init(Benchmark::kCursor | Benchmark::kPrefill);
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

    Benchmark bench{{.value_length = 100'000}};
    bench.init(Benchmark::kWriter);
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
    std::string value;

    Benchmark bench{{.value_length = 100'000}};
    bench.init(Benchmark::kPrefill);
    for (auto _ : state) {
        bench.read(state, &value);
        benchmark::DoNotOptimize(value);
    }
}
BENCHMARK(BM_Read100K)
    ->Arg(kSequential)
    ->Arg(kRandom);

static auto BM_Exists100K(benchmark::State &state) -> void
{
    state.SetLabel("Exists" + access_type_name(state.range(0)) + "100K");

    Benchmark bench{{.value_length = 100'000}};
    bench.init(Benchmark::kPrefill);
    for (auto _ : state) {
        bench.read(state, nullptr);
    }
}
BENCHMARK(BM_Exists100K)
    ->Arg(kSequential)
    ->Arg(kRandom);

BENCHMARK_MAIN();