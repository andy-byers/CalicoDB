#include <filesystem>
#include <random>
#include <benchmark/benchmark.h>
#include <calico/calico.h>

// TODO: Some of these could be specified as commandline parameters.
static constexpr Calico::Size DB_PAYLOAD_SIZE {100};
static constexpr auto DB_VALUE =
    "____________________"
    "____________________"
    "____________________"
    "____________________"
    "________";
static constexpr auto DB_VALUE_SIZE = std::char_traits<Calico::Byte>::length(DB_VALUE);
static_assert(DB_VALUE_SIZE < DB_PAYLOAD_SIZE);

static constexpr auto DB_KEY_SIZE = DB_PAYLOAD_SIZE - DB_VALUE_SIZE;
static constexpr Calico::Size DB_INITIAL_SIZE {10'000};
static constexpr Calico::Size DB_XACT_SIZE {500};
static constexpr auto DB_PATH = "__bench_calico__";

// 3 MiB of page cache + write buffer memory.
static constexpr Calico::Options DB_OPTIONS {
    0x2000,
    0x200000,
    0x100000,
};

struct State {
    static std::default_random_engine s_rng;
    
    static auto random_int() -> int
    {
        std::uniform_int_distribution<int> dist;
        return dist(s_rng);
    }
};

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

static auto do_read(const Calico::Database &db, Calico::Slice key)
{
    if (auto c = db.find(key); c.is_valid()) {
        benchmark::DoNotOptimize(c.key());
        benchmark::DoNotOptimize(c.value());
    }
}

static auto do_write(Calico::Database &db, Calico::Slice key)
{
    benchmark::DoNotOptimize(db.insert(key, DB_VALUE));
}

static auto setup()
{
    std::filesystem::remove_all(DB_PATH);
    Calico::Database db;
    benchmark::DoNotOptimize(db.open(DB_PATH, DB_OPTIONS));
    return db;
}

template<class F>
static auto run_batches(Calico::Database &db, benchmark::State &state, const F &f)
{
    std::optional<Calico::Transaction> xact {db.transaction()};
    Calico::Size i {};

    for (auto _ : state) {
        state.PauseTiming();
        const auto key = make_key<DB_KEY_SIZE>(f(i));
        const auto is_interval = ++i % DB_XACT_SIZE == 0;
        state.ResumeTiming();

        if (is_interval) {
            benchmark::DoNotOptimize(xact->commit());
            xact.emplace(db.transaction());
        }
        do_write(db, key);
    }
    benchmark::DoNotOptimize(xact->commit());
}

static auto BM_SequentialWrites(benchmark::State &state)
{
    auto db = setup();
    run_batches(db, state, [](auto i) {return i;});
}
BENCHMARK(BM_SequentialWrites);

static auto BM_RandomWrites(benchmark::State &state)
{
    auto db = setup();
    run_batches(db, state, [](auto) {return State::random_int();});
}
BENCHMARK(BM_RandomWrites);

static auto run_overwrite(Calico::Database &db, benchmark::State &state)
{
    std::optional<Calico::Transaction> xact {db.transaction()};

    for (Calico::Size i {}; i < DB_INITIAL_SIZE; ++i) {
        const auto key = make_key<DB_KEY_SIZE>(i);
        benchmark::DoNotOptimize(db.insert(key, DB_VALUE));
    }
    benchmark::DoNotOptimize(xact->commit());
    xact.emplace(db.transaction());
    Calico::Size i {};

    for (auto _ : state) {
        state.PauseTiming();
        const auto key = make_key<DB_KEY_SIZE>(State::random_int() % DB_INITIAL_SIZE);
        const auto is_interval = ++i % DB_XACT_SIZE == 0;
        state.ResumeTiming();
        do_write(db, key);

        if (is_interval) {
            benchmark::DoNotOptimize(xact->commit());
            xact.emplace(db.transaction());
        }
    }
    benchmark::DoNotOptimize(xact->commit());
}

static auto BM_Overwrite(benchmark::State& state)
{
    auto db = setup();
    run_overwrite(db, state);
}
BENCHMARK(BM_Overwrite);

static auto setup_with_records(Calico::Size n)
{
    auto db = setup();
    auto xact = db.transaction();
    for (Calico::Size i {}; i < n; ++i) {
        const auto key = make_key<DB_KEY_SIZE>(State::random_int());
        do_write(db, key);
    }
    benchmark::DoNotOptimize(xact.commit());
    return db;
}

static auto BM_SequentialReads(benchmark::State &state)
{
    auto db = setup_with_records(DB_INITIAL_SIZE);
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
    auto db = setup_with_records(DB_INITIAL_SIZE);
    for (auto _ : state) {
        state.PauseTiming();
        const auto key = make_key<DB_KEY_SIZE>(State::random_int());
        state.ResumeTiming();
        do_read(db, key);
    }
}
BENCHMARK(BM_RandomReads);


static auto run_reads_and_writes(benchmark::State& state, int batch_size, int read_fraction, bool is_sequential)
{
    enum class Action {
        READ,
        WRITE,
    };
    auto db = setup_with_records(DB_INITIAL_SIZE);
    std::optional<Calico::Transaction> xact {db.transaction()};
    int i {};

    for (auto _ : state) {
        state.PauseTiming();
        const auto key = make_key<DB_KEY_SIZE>(is_sequential ? i : State::random_int());
        const auto action = State::random_int() % 100 < read_fraction ? Action::READ : Action::WRITE;
        const auto is_interval = ++i % batch_size == 0;
        state.ResumeTiming();

        if (action == Action::READ) {
            do_read(db, key);
        } else {
            do_write(db, key);
        }
        if (is_interval) {
            benchmark::DoNotOptimize(xact->commit());
            xact.emplace(db.transaction());
        }
    }
    benchmark::DoNotOptimize(xact->commit());
}

static auto BM_SequentialReadWrite_25_75(benchmark::State& state)
{
    run_reads_and_writes(state, DB_XACT_SIZE, 25, true);
}
BENCHMARK(BM_SequentialReadWrite_25_75);

static auto BM_SequentialReadWrite_50_50(benchmark::State& state)
{
    run_reads_and_writes(state, DB_XACT_SIZE, 50, true);
}
BENCHMARK(BM_SequentialReadWrite_50_50);

static auto BM_SequentialReadWrite_75_25(benchmark::State& state)
{
    run_reads_and_writes(state, DB_XACT_SIZE, 75, true);
}
BENCHMARK(BM_SequentialReadWrite_75_25);

static auto BM_RandomReadWrite_25_75(benchmark::State& state)
{
    run_reads_and_writes(state, DB_XACT_SIZE, 25, false);
}
BENCHMARK(BM_RandomReadWrite_25_75);

static auto BM_RandomReadWrite_50_50(benchmark::State& state)
{
    run_reads_and_writes(state, DB_XACT_SIZE, 50, false);
}
BENCHMARK(BM_RandomReadWrite_50_50);

static auto BM_RandomReadWrite_75_25(benchmark::State& state)
{
    run_reads_and_writes(state, DB_XACT_SIZE, 75, false);
}
BENCHMARK(BM_RandomReadWrite_75_25);

std::default_random_engine State::s_rng;

auto main(int argc, char *argv[]) -> int
{
    State::s_rng.seed(42);
    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    return 0;
}