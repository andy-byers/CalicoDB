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
                m_path = arg.to_string();
            } else if (arg.starts_with("-page_size=")) {
                arg.advance(11);
                // arg is still null-terminated.
                m_options.page_size = std::stoi(arg.data());
            } else if (arg.starts_with("-page_cache_size=")) {
                arg.advance(17);
                m_options.page_cache_size = std::stoi(arg.data());
            } else if (arg.starts_with("-wal_buffer_size=")) {
                arg.advance(17);
                m_options.wal_buffer_size = std::stoi(arg.data());
            } else if (arg.starts_with("-wal_prefix=")) {
                m_options.wal_prefix = arg;
            } else if (arg.starts_with("-reads=")) {
                arg.advance(14);
                m_reads = std::stoi(arg.data());
            } else if (arg.starts_with("-key_size=")) {
                arg.advance(10);
                m_key_size = std::stoi(arg.data());
            } else if (arg.starts_with("-value_size=")) {
                arg.advance(12);
                m_value = std::string(std::stoi(arg.data()), 'x');
            } else if (arg.starts_with("-batch_size=")) {
                arg.advance(18);
                m_batch_size = std::stoi(arg.data());
            }
        }

        std::error_code code;
        std::filesystem::remove_all(m_path, code);

        CHECK_OK(Database::open(m_path, m_options, &m_db));
    }

    ~Benchmark()
    {
        delete m_db;
    }

    static auto make_key(Size i) -> std::string
    {
        const auto key = std::to_string(i);
        CHECK_TRUE(key.size() <= runner->m_key_size);
        return std::string(runner->m_key_size - key.size(), '0') + key;
    }

    static auto next_key() -> Slice
    {
        runner->m_key = make_key(runner->m_counter++);
        return runner->m_key;
    }

    static auto rand_key() -> Slice
    {
        runner->m_key = runner->m_random.Generate(runner->m_key_size).to_string();
        return runner->m_key;
    }
    
    static auto use_100k_values() -> void
    {
        static constexpr Size KiB {1'024};
        if (runner->m_value.size() != 100 * KiB) {
            runner->m_value = std::string(100 * KiB, 'x');
            runner->m_reads /= KiB;
        }
    }

    static auto reopen(Size n = 0) -> void
    {
        delete runner->m_db;

        CHECK_OK(Database::destroy(runner->m_path, runner->m_options));
        CHECK_OK(Database::open(runner->m_path, runner->m_options, &runner->m_db));
        runner->m_counter = 0;

        for (Size i {}; i < n; ++i) {
            write(next_key());
        }

        CHECK_OK(runner->m_db->commit());
    }

    static auto read(const Slice &key) -> bool
    {
        auto s = runner->m_db->get(key, runner->m_read_buffer);
        runner->m_bytes_read += key.size() + runner->m_read_buffer.size();
        return s.is_ok();
    }

    static auto read(const Cursor &cursor) -> bool
    {
        const auto k = cursor.key();
        const auto v = cursor.value();
        runner->m_bytes_read += k.size() + v.size();
        return true;
    }

    static auto write(const Slice &key) -> void
    {
        runner->m_bytes_written += key.size() + runner->m_value.size();
        const auto s = runner->m_db->put(key, runner->m_value);

#ifdef RUN_CHECKS
        CHECK_OK(s);
#else
        benchmark::DoNotOptimize(s);
#endif // RUN_CHECKS
    }

    static auto erase(const Slice &key) -> bool
    {
        return runner->m_db->erase(key).is_ok();
    }

    static auto maybe_commit(Size &i) -> void
    {
        if (i++ >= runner->m_batch_size) {
            benchmark::DoNotOptimize(runner->m_db->commit());
            runner->m_commits++;
            i = 0;
        }
    }

    static auto db_file_size() -> Size
    {
        auto prefix = runner->m_path;
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
            double(runner->m_bytes_read),
            benchmark::Counter::kIsRate,
            benchmark::Counter::OneK::kIs1024};
    }

    static auto set_write_rate_counter(benchmark::State &state)
    {
        state.counters["WriteRate"] = benchmark::Counter {
            double(runner->m_bytes_written),
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
        state.counters["Commits"] = double(runner->m_commits);
    }

private:
    friend auto BM_SequentialRead(benchmark::State &);
    friend auto BM_RandomRead(benchmark::State &);
    friend auto BM_SequentialWrite(benchmark::State &);
    friend auto BM_RandomWrite(benchmark::State &);
    friend auto BM_Overwrite(benchmark::State &);
    friend auto BM_Erase(benchmark::State &);
    friend auto BM_IterateForward(benchmark::State &);
    friend auto BM_IterateBackward(benchmark::State &);
    friend auto BM_SequentialRead100K(benchmark::State &);
    friend auto BM_RandomRead100K(benchmark::State &);
    friend auto BM_SequentialWrite100K(benchmark::State &);
    friend auto BM_RandomWrite100K(benchmark::State &);

    std::string m_path {DB_PATH};
    std::string m_key;
    std::string m_value {DB_VALUE};
    std::string m_read_buffer;
    Tools::RandomGenerator m_random {4'194'304};
    Options m_options;
    Database *m_db {};
    Size m_key_size {DB_KEY_SIZE};
    Size m_batch_size {DB_BATCH_SIZE};
    Size m_reads {DB_INITIAL_SIZE};
    Size m_bytes_read {};
    Size m_bytes_written {};
    Size m_commits {};
    Size m_counter {};
    bool m_use_fresh_db {};
};

#define Runner Benchmark::runner
std::unique_ptr<Benchmark> Benchmark::runner;

auto BM_RandomRead(benchmark::State &state)
{
    const auto count = Runner->m_reads;
    Runner->reopen(count);
    
    Size found {};

    for (auto _ : state) {
        state.PauseTiming();
        const auto key = Runner->make_key(Runner->m_random.Next(count - 1));
        state.ResumeTiming();

        found += Runner->read(key);
    }

#ifdef RUN_CHECKS
    CHECK_EQ(found, state.iterations());
#endif // RUN_CHECKS

    Runner->set_read_rate_counter(state);
}
BENCHMARK(BM_RandomRead);

auto BM_SequentialRead(benchmark::State &state)
{
    const auto count = Runner->m_reads;
    Runner->reopen(count);
    
    Size found {};

    for (auto _ : state) {
        state.PauseTiming();
        Runner->m_counter %= count;
        const auto key = Runner->next_key();
        state.ResumeTiming();

        found += Runner->read(key);
    }

#ifdef RUN_CHECKS
    CHECK_EQ(found, state.iterations());
#endif // RUN_CHECKS

    Runner->set_read_rate_counter(state);
}
BENCHMARK(BM_SequentialRead);

auto BM_RandomWrite(benchmark::State &state)
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
BENCHMARK(BM_RandomWrite);

auto BM_SequentialWrite(benchmark::State &state)
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
BENCHMARK(BM_SequentialWrite);

auto BM_Overwrite(benchmark::State &state)
{
    const auto count = Runner->m_reads;
    Runner->reopen(count);

    Size i {};
    Slice key;

    for (auto _ : state) {
        state.PauseTiming();
        key = Runner->make_key(Runner->m_random.Next(count));
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
    const auto count = Runner->m_reads;
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
        key = Runner->make_key(Runner->m_random.Next(count));
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
    const auto count = Runner->m_reads;
    Runner->reopen(count);

    auto *cursor = Runner->m_db->new_cursor();

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
    const auto count = Runner->m_reads;
    Runner->reopen(count);

    auto *cursor = Runner->m_db->new_cursor();

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

auto BM_RandomRead100K(benchmark::State &state)
{
    Runner->use_100k_values();
    const auto count = Runner->m_reads;
    Runner->reopen(count);
    
    Size found {};
    
    for (auto _ : state) {
        state.PauseTiming();
        const auto key = Runner->make_key(Runner->m_random.Next(count - 1));
        state.ResumeTiming();

        found += Runner->read(key);
    }

#ifdef RUN_CHECKS
    CHECK_EQ(found, state.iterations());
#endif // RUN_CHECKS

    Runner->set_read_rate_counter(state);
}
BENCHMARK(BM_RandomRead100K);

auto BM_SequentialRead100K(benchmark::State &state)
{
    Runner->use_100k_values();
    const auto count = Runner->m_reads;
    Runner->reopen(count);
    
    Size found {};

    for (auto _ : state) {
        state.PauseTiming();
        Runner->m_counter %= count;
        const auto key = Runner->next_key();
        state.ResumeTiming();

        found += Runner->read(key);
    }

#ifdef RUN_CHECKS
    CHECK_EQ(found, state.iterations());
#endif // RUN_CHECKS

    Runner->set_read_rate_counter(state);
}
BENCHMARK(BM_SequentialRead100K);

auto BM_RandomWrite100K(benchmark::State &state)
{
    Runner->use_100k_values();
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
BENCHMARK(BM_RandomWrite100K);

auto BM_SequentialWrite100K(benchmark::State &state)
{
    Runner->use_100k_values();
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
BENCHMARK(BM_SequentialWrite100K);

} // <anonymous namespace>

auto main(int argc, char *argv[]) -> int
{
    Benchmark::runner = std::make_unique<Benchmark>(argc, argv);

    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    return 0;
}