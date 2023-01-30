#include "bench.h"
#include <benchmark/benchmark.h>
#include <calico/calico.h>
#include <filesystem>
#include <random>

namespace {

// TODO: Use this thing.
const Calico::RandomGenerator rng;

constexpr auto DB_PATH = "__bench_calico__";
using namespace Calico;

// 3 MiB of page cache + write buffer memory.
constexpr Options DB_OPTIONS {
    0x2000,
    0x200000,
    0x100000,
};

auto do_read(const Database &db, Slice key)
{
    std::string value;
    if (auto s = db.get(key, value); s.is_ok()) {
        benchmark::DoNotOptimize(value);
    }
}

auto do_write(Database &db, Slice key)
{
    benchmark::DoNotOptimize(db.put(key, DB_VALUE));
}

auto do_erase(Database &db, const Slice &key)
{
    benchmark::DoNotOptimize(db.erase(key));
}

auto setup(Database &db)
{
    std::filesystem::remove_all(DB_PATH);
    benchmark::DoNotOptimize(db.open(DB_PATH, DB_OPTIONS));
}

auto default_init(Database &, Size)
{

}

template<class GetKeyInteger, class PerformAction>
auto run_batches(Database &db, benchmark::State &state, const GetKeyInteger &get_key, const PerformAction &action, decltype(default_init) init = default_init)
{
    std::optional<Transaction> xact {db.start()};
    Size i {};

    for (auto _ : state) {
        state.PauseTiming();
        init(db, i);
        const auto key = make_key<DB_KEY_SIZE>(get_key(i));
        const auto is_interval = ++i % DB_BATCH_SIZE == 0;
        state.ResumeTiming();

        if (is_interval) {
            benchmark::DoNotOptimize(xact->commit());
            xact.emplace(db.start());
        }
        action(db, key);
    }
    benchmark::DoNotOptimize(xact->commit());
}

auto BM_SequentialWrites(benchmark::State &state)
{
    Database db;
    setup(db);
    run_batches(db, state, [](auto i) {return i;}, do_write);
}
BENCHMARK(BM_SequentialWrites);

auto BM_RandomWrites(benchmark::State &state)
{
    Database db;
    setup(db);
    run_batches(db, state, [](auto) {return State::random_int();}, do_write);
}
BENCHMARK(BM_RandomWrites);

auto BM_Overwrite(benchmark::State& state)
{
    Database db;
    setup(db);
    run_batches(db, state, [](auto) {return State::random_int() % DB_INITIAL_SIZE;}, do_write);
}
BENCHMARK(BM_Overwrite);

auto insert_records(Database &db, Size n)
{
    auto xact = db.start();
    for (Size i {}; i < n; ++i) {
        const auto key = make_key<DB_KEY_SIZE>(State::random_int());
        do_write(db, key);
    }
    benchmark::DoNotOptimize(xact.commit());
}

auto BM_SequentialReads(benchmark::State &state)
{
    Database db;
    setup(db);
    insert_records(db, DB_INITIAL_SIZE);
    auto c = db.cursor();

    for (auto _ : state) {
        state.PauseTiming();
        if (!c.is_valid()) {
            c.seek_first();
        }
        state.ResumeTiming();

        benchmark::DoNotOptimize(c.key());
        benchmark::DoNotOptimize(c.value());
        c.next();
    }
}
BENCHMARK(BM_SequentialReads);

auto BM_RandomReads(benchmark::State& state)
{
    Database db;
    setup(db);
    insert_records(db, DB_INITIAL_SIZE);
    for (auto _ : state) {
        state.PauseTiming();
        const auto key = make_key<DB_KEY_SIZE>(State::random_int());
        state.ResumeTiming();
        do_read(db, key);
    }
}
BENCHMARK(BM_RandomReads);

auto run_reads_and_writes(benchmark::State& state, int batch_size, int read_fraction, bool is_sequential)
{
    enum class Action {
        READ,
        WRITE,
    };
    Database db;
    setup(db);
    insert_records(db, DB_INITIAL_SIZE);
    std::optional<Transaction> xact {db.start()};
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
            xact.emplace(db.start());
        }
    }
    benchmark::DoNotOptimize(xact->commit());
}

auto BM_SequentialReadWrite_25_75(benchmark::State& state)
{
    run_reads_and_writes(state, DB_BATCH_SIZE, 25, true);
}
BENCHMARK(BM_SequentialReadWrite_25_75);

auto BM_SequentialReadWrite_50_50(benchmark::State& state)
{
    run_reads_and_writes(state, DB_BATCH_SIZE, 50, true);
}
BENCHMARK(BM_SequentialReadWrite_50_50);

auto BM_SequentialReadWrite_75_25(benchmark::State& state)
{
    run_reads_and_writes(state, DB_BATCH_SIZE, 75, true);
}
BENCHMARK(BM_SequentialReadWrite_75_25);

auto BM_RandomReadWrite_25_75(benchmark::State& state)
{
    run_reads_and_writes(state, DB_BATCH_SIZE, 25, false);
}
BENCHMARK(BM_RandomReadWrite_25_75);

auto BM_RandomReadWrite_50_50(benchmark::State& state)
{
    run_reads_and_writes(state, DB_BATCH_SIZE, 50, false);
}
BENCHMARK(BM_RandomReadWrite_50_50);

auto BM_RandomReadWrite_75_25(benchmark::State& state)
{
    run_reads_and_writes(state, DB_BATCH_SIZE, 75, false);
}
BENCHMARK(BM_RandomReadWrite_75_25);

auto ensure_records(Database &db, Size)
{
    if (const auto stat = db.statistics(); stat.record_count() < DB_INITIAL_SIZE / 2) {
        for (Size i {}; i < DB_INITIAL_SIZE; ++i) {
            const auto key = make_key<DB_KEY_SIZE>(State::random_int());
            do_write(db, key);
        }
    }
}

auto BM_SequentialErase(benchmark::State& state)
{
    Database db;
    setup(db);
    run_batches(db, state, [](auto) {return 0;}, [](auto &db, auto) {
        auto cursor = db.cursor();
        cursor.seek_first();
        do_erase(db, cursor.key());
    }, ensure_records);
}
BENCHMARK(BM_SequentialErase);

auto BM_RandomErase(benchmark::State& state)
{
    Database db;
    setup(db);
    run_batches(db, state, [](auto) {return State::random_int();}, [](auto &db, const auto &key) {do_erase(db, key);}, ensure_records);
}
BENCHMARK(BM_RandomErase);

} // <anonymous namespace>

std::default_random_engine State::s_rng;

auto main(int argc, char *argv[]) -> int
{
    State::s_rng.seed(42);
    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    return 0;
}