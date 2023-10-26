// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/db.h"
#include "common.h"
#include "logging.h"
#include "test.h"
#include <iomanip>
#include <thread>

namespace calicodb::test
{

class DelayEnv : public EnvWrapper
{
public:
    std::atomic<bool> m_delay_barrier;
    std::atomic<bool> m_delay_sync;

    explicit DelayEnv(Env &env)
        : EnvWrapper(env),
          m_delay_barrier(false),
          m_delay_sync(false)
    {
    }

    ~DelayEnv() override = default;

    auto new_file(const char *filename, OpenMode mode, File *&file_out) -> Status override
    {
        class DelayFile : public FileWrapper
        {
            DelayEnv *m_env;

        public:
            explicit DelayFile(DelayEnv &env, File &base)
                : FileWrapper(base),
                  m_env(&env)
            {
            }

            ~DelayFile() override
            {
                delete m_target;
            }

            auto sync() -> Status override
            {
                if (m_env->m_delay_sync.load(std::memory_order_acquire)) {
                    m_env->sleep(100);
                }
                return m_target->sync();
            }

            auto shm_barrier() -> void override
            {
                if (m_env->m_delay_barrier.load(std::memory_order_acquire)) {
                    m_env->sleep(100);
                }
                m_target->shm_barrier();
            }
        };

        auto s = target()->new_file(filename, mode, file_out);
        if (s.is_ok()) {
            file_out = new DelayFile(*this, *file_out);
        }
        return s;
    }
};

TEST(ConcurrencyTestsTools, BarrierIsReusable)
{
    static constexpr size_t kNumThreads = 20;
    Barrier barrier(kNumThreads + 1);

    std::atomic<int> counter(0);
    std::vector<std::thread> threads;
    for (size_t i = 0; i < kNumThreads; ++i) {
        threads.emplace_back([&counter, &barrier] {
            barrier.wait();
            ++counter;
            barrier.wait();
            barrier.wait();
            --counter;
            barrier.wait();
        });
    }

    ASSERT_EQ(0, counter);
    barrier.wait();
    barrier.wait();

    ASSERT_EQ(kNumThreads, counter);
    barrier.wait();
    barrier.wait();

    ASSERT_EQ(0, counter);
    for (auto &t : threads) {
        t.join();
    }
}

class WaitForever : public BusyHandler
{
public:
    std::atomic<uint64_t> counter = 0;
    explicit WaitForever() = default;
    ~WaitForever() override = default;
    auto exec(unsigned) -> bool override
    {
        counter.fetch_add(1, std::memory_order_relaxed);
        return true;
    }
};

class ConcurrencyTests : public testing::Test
{
protected:
    RandomGenerator m_random;
    std::string m_filename;
    UserPtr<DelayEnv> m_env;
    uint64_t m_sanity_check = 0;

    static WaitForever s_busy_handler;

    explicit ConcurrencyTests()
        : m_filename(get_full_filename(testing::TempDir() + "concurrency")),
          m_env(new DelayEnv(default_env()))
    {
        // Use the default allocator for these tests: the debug allocator generates a ton of
        // contention, causing the tests to timeout sometimes.
        EXPECT_OK(configure(kSetAllocator, nullptr));
        remove_calicodb_files(m_filename);
    }

    ~ConcurrencyTests() override
    {
        EXPECT_OK(configure(kSetAllocator, DebugAllocator::config()));
    }

    auto TearDown() -> void override
    {
        m_env.reset();
        TEST_LOG << "Sanity check: " << m_sanity_check << '\n';
    }

    struct Connection;
    using Operation = std::function<Status(Connection &, Barrier *)>;

    struct Connection {
        Operation op;
        size_t op_args[4] = {};
        const char *filename = nullptr;
        BusyHandler *busy = nullptr;
        Env *env = nullptr;
        Options options;

        DB *db = nullptr;
        std::vector<std::string> result;
    };

    static auto connection_main(Connection &co, Barrier *barrier) -> bool
    {
        if (co.op) {
            EXPECT_OK(co.op(co, barrier));
            return true;
        }
        return false;
    }

    struct ConsistencyTestParameters {
        size_t num_readers = 0;
        size_t num_writers = 0;
        size_t num_checkpointers = 0;

        // These parameters should not be set manually. run_consistency_test() will iterate over various
        // combinations of them.
        size_t num_iterations = 0;
        size_t num_records = 0;
        bool checkpoint_reset = false;
        bool delay_barrier = false;
        bool delay_sync = false;
    };
    auto run_consistency_test_instance(const ConsistencyTestParameters &param) -> void
    {
        TEST_LOG << "ConcurrencyTests.Consistency*\n";
        remove_calicodb_files(m_filename);
        s_busy_handler.counter = 0;

        const auto nt = param.num_readers + param.num_writers + param.num_checkpointers;
        Barrier barrier(nt);

        Connection proto;
        proto.filename = m_filename.c_str();
        proto.env = m_env.get();
        proto.op_args[0] = param.num_iterations;
        proto.op_args[1] = param.num_records;

        std::vector<Connection> connections;
        proto.op = test_writer;
        for (size_t i = 0; i < param.num_writers; ++i) {
            connections.push_back(proto);
        }
        // Write some records to the WAL.
        ASSERT_OK(test_writer(proto, nullptr));

        proto.op = test_checkpointer;
        proto.op_args[1] = param.checkpoint_reset;
        for (size_t i = 0; i < param.num_checkpointers; ++i) {
            connections.push_back(proto);
        }
        // Write the WAL back to the database. If `param.checkpoint_reset` is true, then the WAL will be
        // reset such that writers start at the beginning again.
        ASSERT_OK(test_checkpointer(proto, nullptr));

        proto.op = test_reader;
        proto.op_args[0] = param.num_iterations * 10;
        for (size_t i = 0; i < param.num_readers; ++i) {
            connections.push_back(proto);
        }

        m_env->m_delay_sync.store(param.delay_barrier, std::memory_order_release);
        m_env->m_delay_sync.store(param.delay_sync, std::memory_order_release);

        std::vector<std::thread> threads;
        threads.reserve(connections.size());
        for (auto &co : connections) {
            threads.emplace_back([&co, b = &barrier, t = threads.size()] {
                while (connection_main(co, b)) {
                    // Run until the connection clears its own callback.
                }
            });
        }

        for (auto &t : threads) {
            t.join();
        }

        for (const auto &co : connections) {
            // Check the results.
            for (size_t i = 0; i + 1 < co.result.size(); ++i) {
                uint64_t n;
                Slice slice = co.result[i];
                ASSERT_LE(slice, co.result[i + 1]);
                ASSERT_TRUE(consume_decimal_number(slice, &n));
                ASSERT_TRUE(slice.is_empty());
                m_sanity_check = maxval(m_sanity_check, n);
            }
        }
    }

    // Run multiple concurrent readers, writers, and checkpointers. Each time a writer runs,
    // it increments the numeric representation of each record value (changing 0000012 to
    // 0000013, for example). Each write connection should see monotonically increasing
    // values, since writers are serialized. Read connections might see the same value multiple
    // times in-a-row, but the values should never decrease.
    auto run_consistency_test(const ConsistencyTestParameters &param) -> void
    {
        uint64_t highest_wait_count = 0;
        for (size_t i = 1; i <= 12; i += 4) {
            for (size_t j = 1; j <= 4; ++j) {
                for (size_t k = 1; k <= 4; ++k) {
                    run_consistency_test_instance({
                        param.num_readers,
                        param.num_writers,
                        param.num_checkpointers,
                        i,
                        j,
                        (i & 1) == 0,
                        (j & 1) == 0,
                        (k & 1) == 0,
                    });
                    highest_wait_count = maxval(highest_wait_count, s_busy_handler.counter.load(std::memory_order_relaxed));
                }
            }
        }
        TEST_LOG << "Highest wait count = " << highest_wait_count << '\n';
    }

    [[nodiscard]] static auto reopen_connection(Connection &co) -> Status
    {
        auto opt = co.options;
        opt.env = co.env;
        opt.busy = &s_busy_handler;
        return DB::open(opt, co.filename, co.db);
    }

    static auto barrier_wait(Barrier *barrier) -> void
    {
        if (barrier) {
            barrier->wait();
        }
    }

    // Reader task invariants:
    // 1. If the bucket named "b" exists, it contains co.op_arg records
    // 2. Record keys are monotonically increasing integers starting from 0, serialized
    //    using numeric_key()
    // 3. Each record value is another such serialized integer, however, each value is
    //    identical
    // 4. The record value read by a reader must never decrease between runs
    [[nodiscard]] static auto test_reader(Connection &co, Barrier *barrier) -> Status
    {
        barrier_wait(barrier);

        co.options.create_if_missing = false;
        auto s = reopen_connection(co);
        for (size_t n = 0; s.is_ok() && n < co.op_args[0]; ++n) {
            s = co.db->view([&co](const auto &tx) {
                TestBucket b;
                // Oddly enough, if we name this Status "s", some platforms complain about shadowing,
                // even though s is not captured in this lambda.
                auto t = test_open_bucket(tx, "b", b);
                if (t.is_invalid_argument()) {
                    // Writer hasn't created the bucket yet.
                    return Status::ok();
                } else if (!t.is_ok()) {
                    return t;
                }
                auto c = test_new_cursor(*b);
                // Iterate through the records twice. The same value should be read each time.
                for (size_t i = 0; i < co.op_args[1] * 2; ++i) {
                    // If the bucket exists, then it must contain co.op_arg records (the first writer to run
                    // makes sure of this).
                    const auto key = numeric_key(i % co.op_args[1]);
                    c->find(key);
                    if (!c->is_valid()) {
                        t = c->status();
                        break;
                    } else if (i == 0) {
                        const auto value = c->value();
                        if (!co.result.empty()) {
                            EXPECT_GE(value, co.result.back());
                        }
                        co.result.emplace_back(value.data(), value.size());
                    } else {
                        EXPECT_EQ(c->value(), co.result.back())
                            << "non repeatable read on record " << i % co.op_args[1]
                            << ", round " << i / co.op_args[1];
                    }
                }
                return t.is_not_found() ? Status::ok() : t;
            });
        }

        co.op = nullptr;
        delete co.db;
        // Forgive invalid argument errors that happen when a writer hasn't created the database
        // or bucket yet.
        return s.is_invalid_argument() ? Status::ok() : s;
    }

    // Writer tasks set up invariants on the DB for the reader to check. Each writer
    // either creates or increases co.op_arg[1] records in a bucket named "b". The
    // first writer to run creates the bucket.
    [[nodiscard]] static auto test_writer(Connection &co, Barrier *barrier) -> Status
    {
        barrier_wait(barrier);

        auto s = reopen_connection(co);
        for (size_t n = 0; s.is_ok() && n < co.op_args[0]; ++n) {
            s = co.db->update([&co](auto &tx) {
                TestBucket b;
                auto t = test_create_and_open_bucket(tx, "b", b);
                if (!t.is_ok()) {
                    return t;
                }
                auto c = test_new_cursor(*b);
                for (size_t i = 0; t.is_ok() && i < co.op_args[1]; ++i) {
                    uint64_t result = 0;
                    const auto key = numeric_key(i);
                    c->find(key);
                    if (c->is_valid()) {
                        Slice slice(c->value());
                        EXPECT_TRUE(consume_decimal_number(slice, &result));
                        if (i == 0) {
                            if (!co.result.empty()) {
                                // Writers must never encounter duplicates. Could indicate bad serialization.
                                EXPECT_LT(co.result.back(), c->value());
                            }
                            co.result.emplace_back(c->value().to_string());
                        }
                    } else if (!c->status().is_ok()) {
                        break;
                    }
                    if (t.is_ok()) {
                        ++result;
                        const auto value = numeric_key(result);
                        t = b->put(key, value);
                    }
                }
                EXPECT_OK(t);
                return t;
            });

            if (s.is_busy()) {
                s = Status::ok();
            }
        }

        co.op = nullptr;
        delete co.db;
        return s;
    }

    // Checkpointers just run a single checkpoint on the DB. This should not interfere with the
    // logical contents of the database in any way.
    [[nodiscard]] static auto test_checkpointer(Connection &co, Barrier *barrier) -> Status
    {
        barrier_wait(barrier);

        co.options.create_if_missing = false;
        auto s = reopen_connection(co);
        for (size_t n = 0; s.is_ok() && n < co.op_args[0]; ++n) {
            s = co.db->checkpoint(co.op_args[1] ? kCheckpointRestart
                                                : kCheckpointPassive,
                                  nullptr);

            if (s.is_busy()) {
                s = Status::ok();
            }
        }

        co.op = nullptr;
        delete co.db;
        return s;
    }

    struct CheckpointerTestParameters {
        size_t num_readers = 0;
        size_t num_writers = 0;
        bool auto_checkpoint = false;

        // Set by run_checkpointer_test().
        size_t num_iterations = 0;
        size_t num_records = 0;
        CheckpointMode checkpoint_mode = kCheckpointPassive;
        bool delay_sync = false;
        bool busy_wait = false;
    };

    auto run_checkpointer_test_instance(const CheckpointerTestParameters &param) -> void
    {
        TEST_LOG << "ConcurrencyTests.Checkpointer*\n";
        remove_calicodb_files(m_filename);
        s_busy_handler.counter = 0;

        Connection proto;
        proto.filename = m_filename.c_str();
        proto.env = m_env.get();
        proto.op_args[0] = param.num_iterations;
        proto.op_args[1] = param.num_records;
        // Writers never need to call File::sync(). This is taken care of by the checkpointer
        // thread.
        proto.options.sync_mode = Options::kSyncOff;
        proto.options.auto_checkpoint = param.auto_checkpoint * 1'000;

        std::vector<Connection> connections;
        proto.op = test_writer;
        for (size_t i = 0; i < param.num_writers; ++i) {
            connections.push_back(proto);
        }
        proto.op = [param](auto &co, auto *) {
            // Checkpointers should call File::sync() once on the WAL file before any pages are read,
            // and once on the database file after all pages have been written.
            co.options.sync_mode = Options::kSyncNormal;
            auto s = reopen_connection(co);
            if (s.is_ok()) {
                do {
                    s = co.db->checkpoint(param.checkpoint_mode, nullptr);
                    co.env->sleep(25);
                } while (s.is_busy());

                if (co.op_args[0]-- <= 1) {
                    co.op = nullptr;
                }
                delete co.db;
            }
            return s;
        };
        connections.push_back(proto);

        proto.op = test_reader;
        proto.op_args[0] = param.num_iterations * 10;
        for (size_t i = 0; i < param.num_readers; ++i) {
            connections.push_back(proto);
        }

        m_env->m_delay_sync.store(param.delay_sync, std::memory_order_release);

        std::vector<std::thread> threads;
        threads.reserve(connections.size());
        for (auto &co : connections) {
            threads.emplace_back([&co, t = threads.size()] {
                while (connection_main(co, nullptr)) {
                    // Run until the connection clears its own callback.
                }
            });
        }

        for (auto &t : threads) {
            t.join();
        }

        for (const auto &co : connections) {
            // Check the results.
            for (size_t i = 0; i + 1 < co.result.size(); ++i) {
                uint64_t n;
                Slice slice = co.result[i];
                ASSERT_LE(slice, co.result[i + 1]);
                ASSERT_TRUE(consume_decimal_number(slice, &n));
                ASSERT_TRUE(slice.is_empty());
                m_sanity_check = maxval(m_sanity_check, n);
            }
        }
    }

    // Similar to run_consistency_test(), except sometimes we disable sync and auto
    // checkpoint behavior in the readers and writers. Instead, we have a single
    // background thread run the checkpoints. Also, there are no barriers.
    auto run_checkpointer_test(const CheckpointerTestParameters &param) -> void
    {
        uint64_t highest_wait_count = 0;
        for (size_t num_iterations = 1; num_iterations < 12; num_iterations += 4) {
            for (size_t num_records = 1; num_records <= 4; ++num_records) {
                for (CheckpointMode checkpoint_mode = kCheckpointPassive;
                     checkpoint_mode <= kCheckpointRestart;
                     checkpoint_mode = static_cast<CheckpointMode>(static_cast<int>(checkpoint_mode) + 1)) {
                    for (size_t delay_sync = 0; delay_sync <= 1; ++delay_sync) {
                        for (size_t busy_wait = 0; busy_wait <= 1; ++busy_wait) {
                            run_checkpointer_test_instance({
                                param.num_readers,
                                param.num_writers,
                                param.auto_checkpoint,
                                num_iterations,
                                num_records,
                                checkpoint_mode,
                                delay_sync != 0,
                                busy_wait != 0,
                            });
                            highest_wait_count = maxval(highest_wait_count, s_busy_handler.counter.load(std::memory_order_relaxed));
                        }
                    }
                }
            }
        }
        TEST_LOG << "Highest wait count = " << highest_wait_count << '\n';
    }

    struct SingleWriterParameters {
        size_t num_readers = 0;
        bool auto_checkpoint = false;

        // Set by run_single_writer_test().
        size_t num_iterations = 0;
        size_t num_records = 0;
        bool delay_sync = false;
    };

    auto run_single_writer_test_instance(const SingleWriterParameters &param) -> void
    {
        TEST_LOG << "ConcurrencyTests.SingleWriter*\n";
        remove_calicodb_files(m_filename);
        s_busy_handler.counter = 0;

        Connection proto;
        proto.filename = m_filename.c_str();
        proto.env = m_env.get();
        proto.op_args[0] = param.num_iterations;
        proto.op_args[1] = param.num_records;

        std::vector<Connection> connections;
        proto.op = [](auto &co, auto *) {
            auto s = reopen_connection(co);
            s = co.db->update([&co](auto &tx) {
                TestBucket b;
                auto s = test_create_and_open_bucket(tx, "b", b);
                if (!s.is_ok()) {
                    return s;
                }
                for (size_t iteration = 0; iteration < co.op_args[0]; ++iteration) {
                    std::string value;
                    for (size_t i = 0; i < co.op_args[1]; ++i) {
                        s = b->get(numeric_key(i), &value);
                        if (s.is_ok()) {
                            value = numeric_key(std::stoul(value) + 1);
                        } else if (s.is_not_found()) {
                            value = numeric_key(0);
                            s = Status::ok();
                        }
                        if (s.is_ok()) {
                            s = b->put(numeric_key(i), value);
                        }
                        if (!s.is_ok()) {
                            return s;
                        }
                    }
                    s = tx.commit();
                    if (!s.is_ok()) {
                        return s;
                    }
                }
                return s;
            });
            if (s.is_busy()) {
                s = Status::ok();
            } else {
                co.op = nullptr;
            }
            delete co.db;
            return s;
        };
        connections.push_back(proto);

        proto.op = test_reader;
        proto.op_args[0] = param.num_iterations * 100;
        for (size_t i = 0; i < param.num_readers; ++i) {
            connections.push_back(proto);
        }

        m_env->m_delay_sync.store(param.delay_sync, std::memory_order_release);

        std::vector<std::thread> threads;
        threads.reserve(connections.size());
        for (auto &co : connections) {
            threads.emplace_back([&co] {
                while (connection_main(co, nullptr)) {
                    // Run until the connection clears its own callback.
                }
            });
        }

        for (auto &t : threads) {
            t.join();
        }

        for (const auto &co : connections) {
            // Check the results.
            for (size_t i = 0; i + 1 < co.result.size(); ++i) {
                uint64_t n;
                Slice slice = co.result[i];
                ASSERT_LE(slice, co.result[i + 1]);
                ASSERT_TRUE(consume_decimal_number(slice, &n));
                ASSERT_TRUE(slice.is_empty());
                m_sanity_check = maxval(m_sanity_check, n);
            }
        }
    }

    auto run_single_writer_test(const SingleWriterParameters &param) -> void
    {
        uint64_t highest_wait_count = 0;
        for (size_t num_iterations = 1; num_iterations < 12; num_iterations += 4) {
            for (size_t num_records = 1; num_records <= 4; ++num_records) {
                for (size_t delay_sync = 0; delay_sync <= 1; ++delay_sync) {
                    for (size_t busy_wait = 0; busy_wait <= 1; ++busy_wait) {
                        run_single_writer_test_instance({
                            param.num_readers,
                            param.auto_checkpoint,
                            num_iterations,
                            num_records,
                            delay_sync != 0,
                        });
                        highest_wait_count = maxval(highest_wait_count, s_busy_handler.counter.load(std::memory_order_relaxed));
                    }
                }
            }
        }
        TEST_LOG << "Highest wait count = " << highest_wait_count << '\n';
    }
};

WaitForever ConcurrencyTests::s_busy_handler;

TEST_F(ConcurrencyTests, Consistency0)
{
    // Sanity check, no concurrency.
    run_consistency_test({1, 0, 0});
    run_consistency_test({0, 1, 0});
    run_consistency_test({0, 0, 1});
}

TEST_F(ConcurrencyTests, Consistency1)
{
    run_consistency_test({2, 1, 0});
    run_consistency_test({2, 0, 1});
    run_consistency_test({2, 1, 1});
    run_consistency_test({10, 2, 0});
    run_consistency_test({10, 0, 2});
    run_consistency_test({10, 2, 2});
}

TEST_F(ConcurrencyTests, Consistency2)
{
    run_consistency_test({2, 2, 2});
    run_consistency_test({10, 10, 10});
    run_consistency_test({50, 50, 50});
}

TEST_F(ConcurrencyTests, Checkpointer0)
{
    // Sanity check, no concurrency.
    run_checkpointer_test({0, 0});
}

TEST_F(ConcurrencyTests, Checkpointer1)
{
    run_checkpointer_test({1, 0});
    run_checkpointer_test({1, 1});
}

TEST_F(ConcurrencyTests, Checkpointer2)
{
    run_checkpointer_test({10, 0});
    run_checkpointer_test({2, 10});
    run_checkpointer_test({2, 10});
    run_checkpointer_test({10, 10});
}

TEST_F(ConcurrencyTests, AutoCheckpointer0)
{
    // Sanity check, no concurrency.
    run_checkpointer_test({0, 0, true});
}

TEST_F(ConcurrencyTests, AutoCheckpointer1)
{
    run_checkpointer_test({1, 0, true});
    run_checkpointer_test({1, 1, true});
}

TEST_F(ConcurrencyTests, AutoCheckpointer2)
{
    run_checkpointer_test({10, 0, true});
    run_checkpointer_test({2, 10, true});
    run_checkpointer_test({2, 10, true});
    run_checkpointer_test({10, 10, true});
}

TEST_F(ConcurrencyTests, SingleWriter0)
{
    run_single_writer_test({0, false});
    run_single_writer_test({0, true});
}

TEST_F(ConcurrencyTests, SingleWriter1)
{
    run_single_writer_test({1, false});
    run_single_writer_test({1, true});
}

TEST_F(ConcurrencyTests, SingleWriter2)
{
    run_single_writer_test({10, false});
    run_single_writer_test({10, true});
}

class MultiProcessTests : public testing::TestWithParam<size_t>
{
public:
    const size_t m_num_processes;
    const std::string m_filename;

    explicit MultiProcessTests()
        : m_num_processes(GetParam()),
          m_filename(testing::TempDir() + "calicodb_multi_process_tests")
    {
    }

    ~MultiProcessTests() override = default;

    template <class Test>
    auto run_test_instance(const Test &test)
    {
        std::vector<int> pipes(m_num_processes);
        for (size_t n = 0; n < m_num_processes; ++n) {
            int pipefd[2];
            ASSERT_EQ(pipe(pipefd), 0);
            pipes[n] = pipefd[0];

            const auto pid = fork();
            ASSERT_NE(-1, pid) << strerror(errno);
            if (pid) {
                close(pipefd[1]);
            } else {
                close(pipefd[0]);
                dup2(pipefd[1], STDOUT_FILENO);
                dup2(pipefd[1], STDERR_FILENO);
                test();
                std::exit(testing::Test::HasFailure());
            }
        }
        for (size_t n = 0; n < m_num_processes; ++n) {
            int s;
            const auto pid = wait(&s);
            ASSERT_NE(pid, -1)
                << "wait failed: " << strerror(errno);
            if (!WIFEXITED(s) || WEXITSTATUS(s)) {
                std::string msg;
                for (char buf[256];;) {
                    if (const auto rc = read(pipes[n], buf, sizeof(buf))) {
                        ASSERT_GT(rc, 0) << strerror(errno);
                        msg.append(buf, static_cast<size_t>(rc));
                    } else {
                        break;
                    }
                }
                ADD_FAILURE()
                    << "exited " << (WIFEXITED(s) ? "" : "ab")
                    << "normally with exit status " << WEXITSTATUS(s)
                    << "\nOUTPUT:\n"
                    << msg << "\n";
            }
        }
    }

    static auto writer_thread(const char *filename, BusyHandler *busy)
    {
        DB *db = nullptr;
        Options options;
        options.busy = busy;
        auto s = DB::open(options, filename, db);
        for (size_t i = 0; s.is_ok() && i < 10;) {
            s = db->update([](auto &tx) {
                Status s;
                std::string value;
                auto &b = tx.main_bucket();
                for (size_t i = 0; i < 10; ++i) {
                    s = b.get(numeric_key(i), &value);
                    if (s.is_ok()) {
                        const auto n = std::stoul(value);
                        s = b.put(numeric_key(i), numeric_key(n + 1));
                    } else if (s.is_not_found()) {
                        s = b.put(numeric_key(i), numeric_key(0));
                    }
                    if (!s.is_ok()) {
                        return s;
                    }
                }
                return s;
            });
            if (s.is_ok()) {
                ++i;
            } else if (s.is_busy()) {
                s = Status::ok();
            }
        }
        delete db;
        ASSERT_OK(s);
    }

    static auto reader_thread(const char *filename, BusyHandler *busy)
    {
        DB *db = nullptr;
        Options options;
        options.busy = busy;
        auto s = DB::open(options, filename, db);
        for (size_t i = 0; s.is_ok() && i < 10;) {
            s = db->view([](auto &tx) {
                std::string value;
                auto c = test_new_cursor(tx.main_bucket());
                c->seek_first();
                while (c->is_valid()) {
                    EXPECT_FALSE(c->is_bucket());
                    if (value.empty()) {
                        value = c->value().to_string();
                    } else {
                        EXPECT_EQ(value, c->value().to_string());
                    }
                    c->next();
                }
                return c->status();
            });
            if (s.is_ok()) {
                ++i;
            } else if (s.is_busy()) {
                s = Status::ok();
            }
        }
        delete db;
        ASSERT_OK(s);
    }

    auto run_basic_test(size_t num_threads)
    {
        static WaitForever s_busy_handler;
        remove_calicodb_files(m_filename);
        run_test_instance([num_threads, filename = m_filename] {
            std::vector<std::thread> threads;
            threads.reserve(num_threads);
            const auto num_writers = maxval<size_t>(1, num_threads / 4);
            for (size_t t = 0; t < num_threads; ++t) {
                threads.emplace_back(t < num_writers ? reader_thread : writer_thread,
                                     filename.c_str(), &s_busy_handler);
            }
            for (auto &thread : threads) {
                thread.join();
            }
        });

        reader_thread(m_filename.c_str(), &s_busy_handler);
    }

    struct BankInfo {
        std::vector<uint64_t> accounts;
        uint64_t ledger_index;
    };
    static auto parse_bank_info(const Bucket &values, const Bucket &ledger)
    {
        BankInfo info;
        auto vc = test_new_cursor(values);
        std::string record_accum;
        vc->seek_first();
        while (vc->is_valid()) {
            const auto key = vc->key().to_string();
            const auto total = vc->value().to_string();
            info.accounts.emplace_back(std::stoull(total));
            record_accum.append(key + total);
            vc->next();
        }

        auto lc = test_new_cursor(ledger);
        lc->seek_last();
        EXPECT_TRUE(lc->is_valid());
        const auto record_hash = std::hash<std::string>{}(record_accum);
        EXPECT_EQ(record_hash, std::stoull(lc->value().to_string()));
        info.ledger_index = std::stoull(lc->key().to_string());
        return info;
    }

    static auto bank_thread(const char *filename, BusyHandler *busy, uint64_t micros_to_run)
    {
        const auto update_accounts = [](auto &tx) {
            TestBucket values, ledger;
            ASSERT_OK(test_open_bucket(tx, "values", values));
            ASSERT_OK(test_open_bucket(tx, "ledger", ledger));
            auto info = parse_bank_info(*values, *ledger);

            // Give money to the lucky winner!
            uint64_t award = 0;
            for (auto &a : info.accounts) {
                if (a > 5) {
                    const auto r = static_cast<uint64_t>(rand()) % (a - 5);
                    award += r;
                    a -= r;
                }
            }
            const auto lucky = static_cast<size_t>(rand()) % info.accounts.size();
            info.accounts[lucky] += award;

            auto vc = test_new_cursor(*values);
            vc->seek_first();
            std::string accum;
            for (auto a : info.accounts) {
                const auto total = numeric_key(a);
                ASSERT_OK(values->put(*vc, total));
                accum.append(vc->key().to_string() + total);
                vc->next();
            }

            auto lc = test_new_cursor(*ledger);
            lc->seek_last();
            ASSERT_TRUE(lc->is_valid());
            const auto new_hash = std::hash<std::string>{}(accum);
            ASSERT_OK(ledger->put(numeric_key(info.ledger_index + 1), std::to_string(new_hash)));
        };

        const auto check_accounts = [](const auto &tx) {
            TestBucket values, ledger;
            ASSERT_OK(test_open_bucket(tx, "values", values));
            ASSERT_OK(test_open_bucket(tx, "ledger", ledger));
            auto info1 = parse_bank_info(*values, *ledger);
            CALICODB_DEBUG_DELAY(default_env());
            auto info2 = parse_bank_info(*values, *ledger);
            ASSERT_EQ(info1.accounts, info2.accounts);
            ASSERT_EQ(info1.ledger_index, info2.ledger_index);
        };

        DB *db = nullptr;
        Options options;
        options.busy = busy;
        const auto start_time = std::chrono::system_clock::now();
        auto s = DB::open(options, filename, db);
        while (s.is_ok()) {
            s = db->update([&update_accounts](auto &tx) {
                update_accounts(tx);
                return Status::ok();
            });
            if (s.is_ok()) {
                s = db->view([&check_accounts](auto &tx) {
                    check_accounts(tx);
                    return Status::ok();
                });
            } else if (s.is_busy()) {
                s = Status::ok();
            }
            const auto current_time = std::chrono::system_clock::now();
            const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                current_time - start_time);
            if (micros_to_run < static_cast<uint64_t>(elapsed.count())) {
                break;
            }
        }
        delete db;
        ASSERT_OK(s);
    }

    // Bank test: Accounts are in a bucket named "values", and the ledger is in a bucket named
    // "ledger". There are N accounts, and each account starts out with $10. bank_thread() runs 2
    // transactions. First, a read-write transaction is run, and the account totals are mixed up,
    // keeping the overall total constant. Then, the contents of "values" are hashed and appended
    // to "ledger" (with a sequential integer key). A readonly transaction is then run, where the
    // contents of "values" are checked against the ledger. Every connection created in this test
    // will make a call to bank_thread(), which will loop for roughly 5 seconds. Once the child
    // connections are finished, the main thread makes sure that the total amount of money is
    // still the same as before.
    auto run_bank_test(size_t num_threads)
    {
        DB *db;
        remove_calicodb_files(m_filename);
        auto s = DB::open(Options(), m_filename.c_str(), db);
        ASSERT_OK(db->update([](auto &tx) {
            TestBucket values;
            EXPECT_OK(test_create_and_open_bucket(tx, "values", values));

            std::string accum;
            const auto value = numeric_key(10);
            for (const auto *key : {"a", "b", "c", "d", "e", "f", "g"}) {
                EXPECT_OK(values->put(key, value));
                accum.append(key + value);
            }

            TestBucket ledger;
            const auto hash = std::hash<std::string>{}(accum);
            EXPECT_OK(test_create_and_open_bucket(tx, "ledger", ledger));
            return ledger->put(numeric_key(0), std::to_string(hash));
        }));

        static WaitForever s_busy_handler;
        run_test_instance([num_threads, filename = m_filename] {
            std::vector<std::thread> threads;
            threads.reserve(num_threads);
            for (size_t t = 0; t < num_threads; ++t) {
                threads.emplace_back([&] {
                    bank_thread(filename.c_str(), &s_busy_handler, 5'000'000);
                });
            }
            for (auto &thread : threads) {
                thread.join();
            }
        });

        bank_thread(m_filename.c_str(), &s_busy_handler, 1);

        ASSERT_OK(db->view([](auto &tx) {
            TestBucket values, ledger;
            EXPECT_OK(test_open_bucket(tx, "values", values));
            EXPECT_OK(test_open_bucket(tx, "ledger", ledger));
            auto vc = test_new_cursor(*values);
            vc->seek_first();
            uint64_t total_value = 0;
            while (vc->is_valid()) {
                total_value += std::stoull(vc->value().to_string());
                vc->next();
            }
            EXPECT_EQ(total_value, 70);

            auto lc = test_new_cursor(*ledger);
            lc->seek_last();
            TEST_LOG << "Updates: " << std::stoull(lc->key().to_string()) << '\n';
            return lc->status();
        }));
        delete db;
    }
};

TEST_P(MultiProcessTests, Basic0)
{
    run_basic_test(1);
}

TEST_P(MultiProcessTests, Basic1)
{
    run_basic_test(2);
}

TEST_P(MultiProcessTests, Basic2)
{
    run_basic_test(5);
}

TEST_P(MultiProcessTests, Bank0)
{
    run_bank_test(1);
}

TEST_P(MultiProcessTests, Bank1)
{
    run_bank_test(2);
}

TEST_P(MultiProcessTests, Bank2)
{
    run_bank_test(5);
}

INSTANTIATE_TEST_SUITE_P(
    MultiProcessTests,
    MultiProcessTests,
    ::testing::Values(1, 3, 5));

} // namespace calicodb::test