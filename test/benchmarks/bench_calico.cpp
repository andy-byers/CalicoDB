#include "bench.h"
#include "benchmark/benchmark.h"
#include "calico/calico.h"
#include <filesystem>
#include <fstream>
#include <random>

#define RUN_CHECKS

namespace {

using namespace Calico;

constexpr auto DB_PATH = "__bench_calico__";

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
                readable_records = std::stoi(arg.data());
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

    static auto make_key(Size i) -> std::string
    {
        const auto key = std::to_string(i);
        CHECK_TRUE(key.size() <= runner->m_key_size);
        return std::string(runner->m_key_size - key.size(), '0') + key;
    }

    static auto next_key() -> Slice
    {
        runner->m_key = make_key(runner->counter++);
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
        runner->bytes_read += key.size() + runner->m_read_buffer.size();
        return s.is_ok();
    }

    static auto read(const Cursor &cursor) -> bool
    {
        const auto k = cursor.key();
        const auto v = cursor.value();
        runner->bytes_read += k.size() + v.size();
        return true;
    }

    static auto write(const Slice &key) -> void
    {
        runner->bytes_written += key.size() + runner->value.size();
        const auto s = runner->db->put(key, runner->value);

#ifdef RUN_CHECKS
        CHECK_OK(s);
#else
        benchmark::DoNotOptimize(s);
#endif // RUN_CHECKS
    }

    static auto erase(const Slice &key) -> bool
    {
        return runner->db->erase(key).is_ok();
    }

    static auto maybe_commit(Size &i) -> void
    {
        if (i++ >= runner->m_batch_size) {
            benchmark::DoNotOptimize(runner->db->commit());
            runner->commits++;
            i = 0;
        }
    }

    static auto db_file_size() -> Size
    {
        auto prefix = runner->path;
        if (prefix.back() != '/') {
            prefix += '/';
        }

        std::error_code code;
        const auto size = std::filesystem::file_size(prefix + "data", code);
        CHECK_FALSE(code);

        return size;
    }

    static auto set_read_rate_counter(benchmark::State &state)
    {
        state.counters["ReadRate"] = benchmark::Counter {
            double(runner->bytes_read),
            benchmark::Counter::kIsRate,
            benchmark::Counter::OneK::kIs1024};
    }

    static auto set_write_rate_counter(benchmark::State &state)
    {
        state.counters["WriteRate"] = benchmark::Counter {
            double(runner->bytes_written),
            benchmark::Counter::kIsRate,
            benchmark::Counter::OneK::kIs1024};
    }

    static auto set_file_size_counter(benchmark::State &state)
    {
        state.counters["DbFileSize"] = benchmark::Counter {
            double(runner->db_file_size()),
            {},
            benchmark::Counter::OneK::kIs1024};
    }

    static auto set_commit_counter(benchmark::State &state)
    {
        state.counters["Commits"] = double(runner->commits);
    }

    std::string path {DB_PATH};
    std::string value {DB_VALUE};
    Tools::RandomGenerator random {8 * 1'024 * 1'024};
    Options options;
    Database *db {};
    Size readable_records {DB_INITIAL_SIZE};
    Size bytes_read {};
    Size bytes_written {};
    Size commits {};
    Size counter {};
    bool use_fresh_db {};

private:
    std::string m_key;
    std::string m_read_buffer;
    Size m_key_size {DB_KEY_SIZE};
    Size m_batch_size {DB_BATCH_SIZE};
};

#define Runner Benchmark::runner
std::unique_ptr<Benchmark> Benchmark::runner;

auto BM_RandomReads(benchmark::State &state)
{
    const auto count = Runner->readable_records;
    Runner->reopen(count);
    
    Size found {};

    for (auto _ : state) {
        state.PauseTiming();
        const auto key = Runner->make_key(Runner->random.Next(count - 1));
        state.ResumeTiming();

        found += Runner->read(key);
    }

#ifdef RUN_CHECKS
    CHECK_EQ(found, state.iterations());
#endif // RUN_CHECKS

    Runner->set_read_rate_counter(state);
}
BENCHMARK(BM_RandomReads);

auto BM_SequentialReads(benchmark::State &state)
{
    const auto count = Runner->readable_records;
    Runner->reopen(count);
    
    Size found {};

    for (auto _ : state) {
        state.PauseTiming();
        Runner->counter %= count;
        const auto key = Runner->next_key();
        state.ResumeTiming();

        found += Runner->read(key);
    }

#ifdef RUN_CHECKS
    CHECK_EQ(found, state.iterations());
#endif // RUN_CHECKS

    Runner->set_read_rate_counter(state);
}
BENCHMARK(BM_SequentialReads);

auto BM_RandomWrites(benchmark::State &state)
{
    Runner->reopen();

    Size i {};

    for (auto _ : state) {
        state.PauseTiming();
        const auto key = Runner->rand_key();
        state.ResumeTiming();

        Runner->write(key);
        Benchmark::maybe_commit(i);
    }

    Runner->set_write_rate_counter(state);
    Runner->set_file_size_counter(state);
    Runner->set_commit_counter(state);
}
BENCHMARK(BM_RandomWrites);

auto BM_SequentialWrites(benchmark::State &state)
{
    Size i {};

    for (auto _ : state) {
        state.PauseTiming();
        const auto key = Runner->next_key();
        state.ResumeTiming();

        Runner->write(key);
        Benchmark::maybe_commit(i);
    }

    Runner->set_write_rate_counter(state);
    Runner->set_file_size_counter(state);
    Runner->set_commit_counter(state);
}
BENCHMARK(BM_SequentialWrites);

auto BM_Overwrite(benchmark::State &state)
{
    const auto count = Runner->readable_records;
    Runner->reopen(count);

    Size i {};
    Slice key;

    for (auto _ : state) {
        state.PauseTiming();
        key = Runner->make_key(Runner->random.Next(count));
        state.ResumeTiming();

        Runner->write(key);
        Benchmark::maybe_commit(i);
    }

    Runner->set_write_rate_counter(state);
    Runner->set_file_size_counter(state);
    Runner->set_commit_counter(state);
}
BENCHMARK(BM_Overwrite);

auto BM_Erase(benchmark::State &state)
{
    const auto count = Runner->readable_records;
    Runner->reopen(count);

    std::vector<std::string> erased;

    Size total {};
    Size i {};
    Slice key;

    for (auto _ : state) {
        state.PauseTiming();
        if (erased.size() >= count / 3) {
            for (const auto &k: erased) {
                Runner->write(k);
            }
            erased.clear();
        }
        key = Runner->make_key(Runner->random.Next(count));
        state.ResumeTiming();

        bool found = Runner->erase(key);
        Benchmark::maybe_commit(i);

        state.PauseTiming();
        if (found) {
            erased.emplace_back(key.to_string());
            total++;
        }
        state.ResumeTiming();
    }

    state.counters["RecordsErased"] = benchmark::Counter {
        double(total),
        benchmark::Counter::kIsRate};
}
BENCHMARK(BM_Erase);

auto BM_IterateForward(benchmark::State &state)
{
    const auto count = Runner->readable_records;
    Runner->reopen(count);

    auto *cursor = Runner->db->new_cursor();

    for (auto _ : state) {
        state.PauseTiming();
        if (!cursor->is_valid()) {
            cursor->seek_first();
        }
        state.ResumeTiming();

        Runner->read(*cursor);
        cursor->next();
    }
    delete cursor;

    Runner->set_read_rate_counter(state);
}
BENCHMARK(BM_IterateForward);


auto BM_IterateBackward(benchmark::State &state)
{
    const auto count = Runner->readable_records;
    Runner->reopen(count);

    auto *cursor = Runner->db->new_cursor();

    for (auto _ : state) {
        state.PauseTiming();
        if (!cursor->is_valid()) {
            cursor->seek_last();
        }
        state.ResumeTiming();

        Runner->read(*cursor);
        cursor->previous();
    }
    delete cursor;

    Runner->set_read_rate_counter(state);
}
BENCHMARK(BM_IterateBackward);

} // <anonymous namespace>

auto main(int argc, char *argv[]) -> int
{
    Benchmark::runner = std::make_unique<Benchmark>(argc, argv);

    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    return 0;
}