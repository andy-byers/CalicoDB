#include "bench.h"
#include "benchmark/benchmark.h"
#include "calico/calico.h"
#include <filesystem>
#include <random>

#define RUN_CHECKS

namespace {

using namespace Calico;

constexpr auto DB_PATH = "__bench_calico__";

const Calico::Tools::RandomGenerator rng { 4 * 1'024 * 1'024};

// 4 MiB of page cache + write buffer memory.
constexpr Options DB_OPTIONS {
    0x2000,
    0x200000,
    0x200000,
};

class Benchmark {
public:
    static std::unique_ptr<Benchmark> runner;

    Benchmark(int argc, char **argv)
    {
        for (int i {1}; i < argc; ++i) {
            if (Slice arg {argv[i]}; arg.starts_with("-db_path=")) {
                arg.advance(9);
                // arg is still null-terminated.
                path = arg.to_string();
            } else if (arg.starts_with("-page_size=")) {
                arg.advance(11);
                // arg is still null-terminated.
                options.page_size = std::stoi(arg.data());
            } else if (arg.starts_with("-page_cache_size=")) {
                arg.advance(17);
                options.page_cache_size = std::stoi(arg.data());
            } else if (arg.starts_with("-wal_buffer_size=")) {
                arg.advance(17);
                options.wal_buffer_size = std::stoi(arg.data());
            } else if (arg.starts_with("-wal_prefix=")) {
                options.wal_prefix = arg;
            } else if (arg.starts_with("-record_count=")) {
                arg.advance(14);
                record_count = std::stoi(arg.data());
            } else if (arg.starts_with("-key_size=")) {
                arg.advance(10);
                m_key_size = std::stoi(arg.data());
            } else if (arg.starts_with("-value_size=")) {
                arg.advance(12);
                value = std::string(std::stoi(arg.data()), 'x');
            } else if (arg.starts_with("-batch_size=")) {
                arg.advance(18);
                m_batch_size = std::stoi(arg.data());
            } else if (arg == "--use_fresh_db") {
                use_fresh_db = true;
            }
        }

        std::error_code code;
        std::filesystem::remove_all(path, code);

        CHECK_OK(Database::open(path, options, &db));
    }

    ~Benchmark()
    {
        delete db;
    }

    static auto next_key() -> Slice
    {
        const auto key = std::to_string(runner->counter++);
        CHECK_TRUE(key.size() <= runner->m_key_size);
        runner->m_key = std::string(runner->m_key_size - key.size(), '0') + key;
        return runner->m_key;
    }

    static auto rand_key() -> Slice
    {
        runner->m_key = runner->random.Generate(runner->m_key_size).to_string();
        return runner->m_key;
    }

    static auto reopen(Size n = 0) -> void
    {
        delete runner->db;
        if (runner->use_fresh_db) {
            CHECK_OK(Database::destroy(runner->path, runner->options));
        }
        CHECK_OK(Database::open(runner->path, runner->options, &runner->db));
        runner->counter = 0;

        for (Size i {}; i < n; ++i) {
            write(next_key());
        }
        CHECK_OK(runner->db->commit());
    }

    static auto read(const Slice &key) -> bool
    {
        auto s = runner->db->get(key, runner->m_read_buffer);
        benchmark::DoNotOptimize(runner->m_read_buffer);
        return s.is_ok();
    }

    static auto write(const Slice &key) -> void
    {
        benchmark::DoNotOptimize(runner->db->put(key, runner->value));
    }

    static auto maybe_commit(Size &i) -> void
    {
        if (i++ >= runner->m_batch_size) {
            benchmark::DoNotOptimize(runner->db->commit());
            i = 0;
        }
    }

    std::string path {DB_PATH};
    std::string value {DB_VALUE};
    Tools::RandomGenerator random {8 * 1'024 * 1'024};
    Options options;
    Database *db {};
    Size record_count {DB_INITIAL_SIZE};
    Size counter {};
    bool use_fresh_db {};

private:
    std::string m_key;
    std::string m_read_buffer;
    Size m_key_size {DB_KEY_SIZE};
    Size m_batch_size {DB_BATCH_SIZE};
};

std::unique_ptr<Benchmark> Benchmark::runner;

auto BM_RandomReads_(benchmark::State &state)
{
    const auto count = Benchmark::runner->record_count;
    Benchmark::runner->reopen(count);
    
    Slice key;
    Size found {};

    for (auto _ : state) {
        state.PauseTiming();
        Benchmark::runner->counter = Benchmark::runner->random.Next(count - 1);
        key = Benchmark::runner->next_key();
        state.ResumeTiming();

        found += Benchmark::runner->read(key);
    }

#ifdef RUN_CHECKS
    CHECK_EQ(found, state.iterations());
#endif // RUN_CHECKS
}
BENCHMARK(BM_RandomReads_);

auto BM_SequentialReads_(benchmark::State &state)
{
    const auto count = Benchmark::runner->record_count;
    Benchmark::runner->reopen(count);
    
    Slice key;
    Size found {};

    for (auto _ : state) {
        state.PauseTiming();
        if (Benchmark::runner->counter >= count) {
            Benchmark::runner->counter = 0;
        }
        key = Benchmark::runner->next_key();
        state.ResumeTiming();
        found += Benchmark::runner->read(key);
    }

#ifdef RUN_CHECKS
    CHECK_EQ(found, state.iterations());
#endif // RUN_CHECKS
}
BENCHMARK(BM_SequentialReads_);

auto BM_RandomWrites_(benchmark::State &state)
{
    Benchmark::runner->reopen();

    Size i {};
    Slice key;
    for (auto _ : state) {
        state.PauseTiming();
        key = Benchmark::runner->rand_key();
        state.ResumeTiming();
        Benchmark::runner->write(key);
        Benchmark::maybe_commit(i);
    }

//#ifdef RUN_CHECKS
//    auto *cursor = Benchmark::runner->db->new_cursor();
//    cursor->seek_first();
//
//    i = 0;
//    for (; cursor->is_valid(); ++i) {
//        CHECK_EQ(cursor->value(), Benchmark::runner->value);
//        cursor->next();
//    }
//    if (Benchmark::runner->use_fresh_db) {
//        CHECK_EQ(i, state.iterations());
//    } else {
//        CHECK_TRUE(i >= state.iterations());
//    }
//    delete cursor;
//#endif // RUN_CHECKS
}
BENCHMARK(BM_RandomWrites_);

auto BM_SequentialWrites_(benchmark::State &state)
{
    Size i {};
    Slice key;
    std::string value;
    for (auto _ : state) {
        state.PauseTiming();
        key = Benchmark::runner->next_key();
        state.ResumeTiming();
        Benchmark::runner->write(key);
        Benchmark::maybe_commit(i);
    }

//#ifdef RUN_CHECKS
//    // Make sure every record was actually inserted.
//    auto *cursor = Benchmark::runner->db->new_cursor();
//    cursor->seek_first();
//
//    Size i {};
//    while (cursor->is_valid()) {
//        CHECK_EQ(cursor->value(), Benchmark::runner->value);
//        cursor->next();
//    }
//    if (Benchmark::runner->use_fresh_db) {
//        CHECK_EQ(i, state.iterations());
//    } else {
//        CHECK_TRUE(i >= state.iterations());
//    }
//    delete cursor;
//#endif // RUN_CHECKS
}
BENCHMARK(BM_SequentialWrites_);

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

auto setup(Database **db)
{
    std::filesystem::remove_all(DB_PATH);
    CHECK_OK(Database::open(DB_PATH, DB_OPTIONS, db));
}

auto default_init(Database &, Size)
{

}

template<class GetKey, class PerformAction>
auto run_batches(Database &db, benchmark::State &state, const GetKey &get_key, const PerformAction &action, decltype(default_init) init = default_init)
{
    Size i {};

    for (auto _ : state) {
        state.PauseTiming();
        init(db, i);
        const auto key = get_key(i);
        const auto is_interval = ++i % DB_BATCH_SIZE == 0;
        state.ResumeTiming();

        if (is_interval) {
            benchmark::DoNotOptimize(db.commit());
        }
        action(db, key);
    }
    benchmark::DoNotOptimize(db.commit());
}

auto BM_SequentialWrites(benchmark::State &state)
{
    Database *db;
    setup(&db);
    run_batches(*db, state, [](auto i) {return Tools::integral_key<DB_KEY_SIZE>(i);}, do_write);

#ifdef RUN_CHECKS
    // Make sure every record was actually inserted.
    auto *cursor = db->new_cursor();
    cursor->seek_first();

    Size i {};
    while (cursor->is_valid()) {
        assert(cursor->key() == Tools::integral_key<DB_KEY_SIZE>(i++));
        assert(cursor->value() == DB_VALUE);
        cursor->next();
    }
    assert(i == state.iterations());
#endif // RUN_CHECKS

    delete db;
}
BENCHMARK(BM_SequentialWrites);

auto BM_RandomWrites(benchmark::State &state)
{
    Database *db;
    setup(&db);
    run_batches(*db, state, [](auto) {return rng.Generate(DB_KEY_SIZE);}, do_write);

#ifdef RUN_CHECKS
    auto *cursor = db->new_cursor();
    cursor->seek_first();

    Size i {};
    for (; cursor->is_valid(); ++i) {
        assert(cursor->value() == DB_VALUE);
        cursor->next();
    }
    assert(i == state.iterations());
#endif // RUN_CHECKS

    delete db;
}
BENCHMARK(BM_RandomWrites);

auto BM_Overwrite(benchmark::State& state)
{
    Database *db;
    setup(&db);
    run_batches(*db, state, [](auto) {return std::to_string(rng.Next<Size>(DB_INITIAL_SIZE));}, do_write);
    delete db;
}
BENCHMARK(BM_Overwrite);

auto insert_records(Database &db, Size n)
{
    for (Size i {}; i < n; ++i) {
        const auto key = Tools::integral_key<DB_KEY_SIZE>(i);
        do_write(db, key);
    }
    benchmark::DoNotOptimize(db.commit());
}

auto BM_SequentialReads(benchmark::State &state)
{
    Database *db;
    setup(&db);
    insert_records(*db, DB_INITIAL_SIZE);
    auto *c = db->new_cursor();

    for (auto _ : state) {
        state.PauseTiming();
        if (!c->is_valid()) {
            c->seek_first();
        }
        state.ResumeTiming();

        benchmark::DoNotOptimize(c->key());
        benchmark::DoNotOptimize(c->value());
        c->next();
    }
    delete c;
}
BENCHMARK(BM_SequentialReads);

auto BM_RandomReads(benchmark::State& state)
{
    Database *db;
    setup(&db);
    insert_records(*db, DB_INITIAL_SIZE);
    for (auto _ : state) {
        state.PauseTiming();
        const auto key = Tools::integral_key<DB_KEY_SIZE>(rand() % DB_INITIAL_SIZE);
        state.ResumeTiming();
        do_read(*db, key);
    }
}
BENCHMARK(BM_RandomReads);

auto run_reads_and_writes(benchmark::State& state, int batch_size, int read_fraction, bool is_sequential)
{
    enum class Action {
        READ,
        WRITE,
    };
    Database *db;
    setup(&db);
    insert_records(*db, DB_INITIAL_SIZE);
    int i {};

    for (auto _ : state) {
        state.PauseTiming();
        const auto key = Tools::integral_key<DB_KEY_SIZE>(is_sequential ? i : rand());
        const auto action = rand() % 100 < read_fraction ? Action::READ : Action::WRITE;
        const auto is_interval = ++i % batch_size == 0;
        state.ResumeTiming();

        if (action == Action::READ) {
            do_read(*db, key);
        } else {
            do_write(*db, key);
        }
        if (is_interval) {
            benchmark::DoNotOptimize(db->commit());
        }
    }
    benchmark::DoNotOptimize(db->commit());
    delete db;
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
    std::string property;
    (void)db.get_property("calico.count.records", property);
    if (std::stoi(property) < DB_INITIAL_SIZE / 2) {
        for (Size i {}; i < DB_INITIAL_SIZE; ++i) {
            const auto key = Tools::integral_key<DB_KEY_SIZE>(rng.Next<Size>(1'000'000));
            do_write(db, key);
        }
    }
}

auto BM_SequentialErase(benchmark::State& state)
{
    Database *db;
    setup(&db);
    run_batches(*db, state, [](auto) {return 0;}, [](auto &db, auto) {
        auto *cursor = db.new_cursor();
        cursor->seek_first();
        do_erase(db, cursor->key());
        delete cursor;
    }, ensure_records);
}
BENCHMARK(BM_SequentialErase);

auto BM_RandomErase(benchmark::State& state)
{
    Database *db;
    setup(&db);

    run_batches(
        *db,
        state,
        [&db](auto i) {
            auto *cursor = db->new_cursor();
            while (!cursor->is_valid()) {
                cursor->seek(rng.Generate(DB_KEY_SIZE));
            }
            auto key = cursor->key().to_string();
            delete cursor;
            return key;

        },
        [](auto &db, const auto &key) {do_erase(db, key);},
        ensure_records);
}
BENCHMARK(BM_RandomErase);

} // <anonymous namespace>

auto main(int argc, char *argv[]) -> int
{
    Benchmark::runner = std::make_unique<Benchmark>(argc, argv);

    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    return 0;
}