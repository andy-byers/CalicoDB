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

static auto do_erase(Calico::Database &db, Calico::Slice key)
{
    benchmark::DoNotOptimize(db.erase(key));
}

static auto do_erase(Calico::Database &db, const Calico::Cursor &c)
{
    benchmark::DoNotOptimize(db.erase(c));
}

static auto setup()
{
    std::filesystem::remove_all(DB_PATH);
    Calico::Database db;
    benchmark::DoNotOptimize(db.open(DB_PATH, DB_OPTIONS));
    return db;
}

static auto default_init(Calico::Database &, Calico::Size)
{

}

template<class GetKeyInteger, class PerformAction>
static auto run_batches(Calico::Database &db, benchmark::State &state, const GetKeyInteger &get_key, const PerformAction &action, decltype(default_init) init = default_init)
{
    std::optional<Calico::Transaction> xact {db.transaction()};
    Calico::Size i {};

    for (auto _ : state) {
        state.PauseTiming();
        init(db, i);
        const auto key = make_key<DB_KEY_SIZE>(get_key(i));
        const auto is_interval = ++i % DB_XACT_SIZE == 0;
        state.ResumeTiming();

        if (is_interval) {
            benchmark::DoNotOptimize(xact->commit());
            xact.emplace(db.transaction());
        }
        action(db, key);
    }
    benchmark::DoNotOptimize(xact->commit());
}

static auto BM_SequentialWrites(benchmark::State &state)
{
    auto db = setup();
    run_batches(db, state, [](auto i) {return i;}, do_write);
}
BENCHMARK(BM_SequentialWrites);

static auto BM_RandomWrites(benchmark::State &state)
{
    auto db = setup();
    run_batches(db, state, [](auto) {return State::random_int();}, do_write);
}
BENCHMARK(BM_RandomWrites);

static auto BM_Overwrite(benchmark::State& state)
{
    auto db = setup();
    run_batches(db, state, [](auto) {return State::random_int() % DB_INITIAL_SIZE;}, do_write);
}
BENCHMARK(BM_Overwrite);

static auto insert_records(Calico::Database &db, Calico::Size n)
{
    auto xact = db.transaction();
    for (Calico::Size i {}; i < n; ++i) {
        const auto key = make_key<DB_KEY_SIZE>(State::random_int());
        do_write(db, key);
    }
    benchmark::DoNotOptimize(xact.commit());
}

static auto BM_SequentialReads(benchmark::State &state)
{
    auto db = setup();
    insert_records(db, DB_INITIAL_SIZE);
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
    auto db = setup();
    insert_records(db, DB_INITIAL_SIZE);
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
    auto db = setup();
    insert_records(db, DB_INITIAL_SIZE);
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

static auto ensure_records(Calico::Database &db, Calico::Size)
{
    if (const auto stat = db.statistics(); stat.record_count() < DB_INITIAL_SIZE / 2) {
        for (Calico::Size i {}; i < DB_INITIAL_SIZE; ++i) {
            const auto key = make_key<DB_KEY_SIZE>(State::random_int());
            do_write(db, key);
        }
    }
}

static auto BM_SequentialErase(benchmark::State& state)
{
    auto db = setup();
    run_batches(db, state, [](auto) {return 0;}, [](auto &db, auto) {do_erase(db, db.first());}, ensure_records);
}
BENCHMARK(BM_SequentialErase);

static auto BM_RandomErase(benchmark::State& state)
{
    auto db = setup();
    run_batches(db, state, [](auto) {return 0;}, [](auto &db, auto key) {do_erase(db, key);}, ensure_records);
}
BENCHMARK(BM_RandomErase);

std::default_random_engine State::s_rng;

auto main(int argc, char *argv[]) -> int
{
    State::s_rng.seed(42);
    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    return 0;
}