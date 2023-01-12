#include <benchmark/benchmark.h>
#include <calico/calico.h>
#include <filesystem>

static constexpr auto DB_PATH = "__bench_calico__";
static constexpr Calico::Size DB_KEY_SIZE {12};
static constexpr Calico::Size DB_VALUE_SIZE {88};
static constexpr Calico::Size DB_RECORD_COUNT {10'000};

// Default database options.
static constexpr Calico::Options DB_OPTIONS;

const std::string g_value(DB_VALUE_SIZE, ' ');

template<std::size_t Length = DB_KEY_SIZE>
static auto make_key(Calico::Size key) -> std::string
{
    auto key_string = std::to_string(key);
    if (key_string.size() == Length) {
        return key_string;
    } else if (key_string.size() > Length) {
        return key_string.substr(0, Length);
    } else {
        return std::string(Length - key_string.size(), '0') + key_string;
    }
}

static auto setup()
{
    std::filesystem::remove_all(DB_PATH);
    Calico::Database db;

    if (const auto s = db.open(DB_PATH, DB_OPTIONS); !s.is_ok()) {
        std::fprintf(stderr, "error: %s", s.what().data());
        std::exit(EXIT_FAILURE);
    }
    return db;
}

static auto run_sequential_writes(Calico::Database &db, benchmark::State &state)
{
    Calico::Size i {};

    for (auto _ : state) {
        state.PauseTiming();
        const auto key = make_key<DB_KEY_SIZE>(i++);
        state.ResumeTiming();

        benchmark::DoNotOptimize(db.insert(key, g_value));
    }
}

static auto BM_AtomicSequentialWrites(benchmark::State &state)
{
    auto db = setup();
    run_sequential_writes(db, state);
}
BENCHMARK(BM_AtomicSequentialWrites);

static auto BM_SequentialWrites(benchmark::State &state)
{
    auto db = setup();
    auto xact = db.transaction();
    run_sequential_writes(db, state);
    benchmark::DoNotOptimize(xact.commit());
}
BENCHMARK(BM_SequentialWrites);

static auto run_random_writes(Calico::Database &db, benchmark::State &state)
{
    for (auto _ : state) {
        state.PauseTiming();
        auto key = make_key<DB_KEY_SIZE>(rand());
        state.ResumeTiming();
        benchmark::DoNotOptimize(db.insert(key, g_value).is_ok());
    }
}

static auto BM_AtomicRandomWrites(benchmark::State &state)
{
    auto db = setup();
    run_random_writes(db, state);
}
BENCHMARK(BM_AtomicRandomWrites);

static auto BM_RandomWrites(benchmark::State& state)
{
    auto db = setup();
    auto xact = db.transaction();
    run_random_writes(db, state);
    benchmark::DoNotOptimize(xact.commit().is_ok());
}
BENCHMARK(BM_RandomWrites);

static auto run_overwrite(Calico::Database &db, benchmark::State &state)
{
    for (Calico::Size i {}; i < DB_RECORD_COUNT; ++i) {
        auto key = make_key<DB_KEY_SIZE>(i);
        benchmark::DoNotOptimize(db.insert(key, g_value).is_ok());
    }

    for (auto _ : state) {
        state.PauseTiming();
        auto key = make_key<DB_KEY_SIZE>(rand() % DB_RECORD_COUNT);
        state.ResumeTiming();
        benchmark::DoNotOptimize(db.insert(key, g_value).is_ok());
    }
}

static auto BM_AtomicOverwrite(benchmark::State& state)
{
    auto db = setup();
    run_overwrite(db, state);
}
BENCHMARK(BM_AtomicOverwrite);

static auto BM_Overwrite(benchmark::State& state)
{
    auto db = setup();
    auto xact = db.transaction();
    run_overwrite(db, state);
    benchmark::DoNotOptimize(xact.commit().is_ok());
}
BENCHMARK(BM_Overwrite);

static auto setup_with_records(Calico::Size n)
{
    auto db = setup();
    auto xact = db.transaction();
    for (Calico::Size i {}; i < n; ++i) {
        auto key = make_key<DB_KEY_SIZE>(rand());
        benchmark::DoNotOptimize(db.insert(key, g_value).is_ok());
    }
    benchmark::DoNotOptimize(xact.commit().is_ok());
    return db;
}

static auto BM_SequentialReads(benchmark::State &state)
{
    auto db = setup_with_records(DB_RECORD_COUNT);
    auto c = db.first();

    for (auto _ : state) {
        state.PauseTiming();
        if (!c.is_valid())
            c = db.first();
        state.ResumeTiming();

        benchmark::DoNotOptimize(c.key());
        benchmark::DoNotOptimize(c.value());
        c++;
    }
}
BENCHMARK(BM_SequentialReads);

static auto BM_RandomReads(benchmark::State& state)
{
    auto db = setup_with_records(DB_RECORD_COUNT);
    for (auto _ : state) {
        state.PauseTiming();
        auto key = make_key<DB_KEY_SIZE>(rand());
        state.ResumeTiming();

        if (auto c = db.find(key); c.is_valid()) {
            benchmark::DoNotOptimize(c.key());
            benchmark::DoNotOptimize(c.value());
        }
    }
}
BENCHMARK(BM_RandomReads);

BENCHMARK_MAIN();