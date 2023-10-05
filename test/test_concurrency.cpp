// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/db.h"
#include "common.h"
#include "logging.h"
#include "test.h"
#include <filesystem>
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

class ConcurrencyTests : public testing::Test
{
protected:
    RandomGenerator m_random;
    std::string m_filename;
    UserPtr<DelayEnv> m_env;
    uint64_t m_sanity_check = 0;

    static class WaitForever : public BusyHandler
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
    } s_busy_handler;

    explicit ConcurrencyTests()
        : m_filename(get_full_filename(testing::TempDir() + "concurrency")),
          m_env(new DelayEnv(default_env()))
    {
        remove_calicodb_files(m_filename);
    }

    ~ConcurrencyTests() override = default;

    auto TearDown() -> void override
    {
        m_env.reset();
        DebugAllocator::set_limit(0);
        // All resources should have been cleaned up by now, even though the Env is not deleted.
        ASSERT_EQ(DebugAllocator::bytes_used(), 0) << "leaked " << std::setprecision(4)
                                                   << static_cast<double>(DebugAllocator::bytes_used()) / (1'024.0 * 1'024)
                                                   << " MiB";
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
        proto.op = test_reader;
        for (size_t i = 0; i < param.num_readers; ++i) {
            connections.emplace_back(proto);
        }
        proto.op = test_writer;
        for (size_t i = 0; i < param.num_writers; ++i) {
            connections.emplace_back(proto);
        }
        // Write some records to the WAL.
        ASSERT_OK(test_writer(proto, nullptr));

        proto.op = test_checkpointer;
        proto.op_args[1] = param.checkpoint_reset;
        for (size_t i = 0; i < param.num_checkpointers; ++i) {
            connections.emplace_back(proto);
        }
        // Write the WAL back to the database. If `param.checkpoint_reset` is true, then the WAL will be
        // reset such that writers start at the beginning again.
        ASSERT_OK(test_checkpointer(proto, nullptr));

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
                auto slice = to_slice(co.result[i]);
                ASSERT_LE(slice, to_slice(co.result[i + 1]));
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
        for (size_t i = 1; i <= 4; ++i) {
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
            barrier_wait(barrier);

            s = co.db->run(ReadOptions(), [&co, barrier](const auto &tx) {
                barrier_wait(barrier);

                TestCursor c;
                // Oddly enough, if we name this Status "s", some platforms complain about shadowing,
                // even though s is not captured in this lambda.
                auto t = test_open_bucket(tx, "b", c);
                if (t.is_invalid_argument()) {
                    // Writer hasn't created the bucket yet.
                    return Status::ok();
                } else if (!t.is_ok()) {
                    return t;
                }
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
                        EXPECT_EQ(c->value(), to_slice(co.result.back())) << "non repeatable read";
                    }
                }
                return t.is_not_found() ? Status::ok() : t;
            });
        }

        barrier_wait(barrier);
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
            barrier_wait(barrier);

            s = co.db->run(WriteOptions(), [&co, barrier](auto &tx) {
                barrier_wait(barrier);

                TestCursor c;
                auto t = test_create_and_open_bucket(tx, BucketOptions(), "b", c);
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
                        t = tx.put(*c, key, value);
                    }
                }
                EXPECT_OK(t);
                return t;
            });

            if (s.is_busy()) {
                barrier_wait(barrier);
                s = Status::ok();
            }
        }

        barrier_wait(barrier);
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
            barrier_wait(barrier);
            barrier_wait(barrier);
            s = co.db->checkpoint(co.op_args[1] ? kCheckpointRestart
                                                : kCheckpointPassive,
                                  nullptr);

            if (s.is_busy()) {
                s = Status::ok();
            }
        }

        barrier_wait(barrier);
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
            connections.emplace_back(proto);
        }
        proto.op = test_reader;
        for (size_t i = 0; i < param.num_readers; ++i) {
            connections.emplace_back(proto);
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
        connections.emplace_back(proto);

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
                auto slice = to_slice(co.result[i]);
                ASSERT_LE(slice, to_slice(co.result[i + 1]));
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
        for (size_t num_iterations = 1; num_iterations <= 4; ++num_iterations) {
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
};

ConcurrencyTests::WaitForever ConcurrencyTests::s_busy_handler;

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
}

TEST_F(ConcurrencyTests, Consistency3)
{
    run_consistency_test({100, 10, 10});
    run_consistency_test({10, 100, 10});
    run_consistency_test({10, 10, 100});
}

TEST_F(ConcurrencyTests, Consistency4)
{
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
    run_checkpointer_test({2, 0});
    run_checkpointer_test({2, 1});
    run_checkpointer_test({1, 2});
    run_checkpointer_test({2, 2});
}

TEST_F(ConcurrencyTests, Checkpointer3)
{
    run_checkpointer_test({10, 0});
    run_checkpointer_test({10, 2});
    run_checkpointer_test({2, 10});
    run_checkpointer_test({10, 10});
}

TEST_F(ConcurrencyTests, Checkpointer4)
{
    run_checkpointer_test({50, 0});
    run_checkpointer_test({10, 50});
    run_checkpointer_test({10, 50});
    run_checkpointer_test({50, 50});
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
    run_checkpointer_test({2, 0, true});
    run_checkpointer_test({2, 1, true});
    run_checkpointer_test({1, 2, true});
    run_checkpointer_test({2, 2, true});
}

TEST_F(ConcurrencyTests, AutoCheckpointer3)
{
    run_checkpointer_test({10, 0, true});
    run_checkpointer_test({10, 2, true});
    run_checkpointer_test({2, 10, true});
    run_checkpointer_test({10, 10, true});
}

TEST_F(ConcurrencyTests, AutoCheckpointer4)
{
    run_checkpointer_test({50, 0, true});
    run_checkpointer_test({10, 50, true});
    run_checkpointer_test({10, 50, true});
    run_checkpointer_test({50, 50, true});
}

} // namespace calicodb::test