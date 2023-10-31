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
    explicit DelayEnv(Env &env)
        : EnvWrapper(env)
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
                if (m_env->rand() % 8 == 0) {
                    m_env->sleep(100);
                }
                return m_target->sync();
            }

            auto shm_barrier() -> void override
            {
                if (m_env->rand() % 8 == 0) {
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
        : m_filename(get_full_filename(testing::TempDir() + "calicodb_test_concurrency")),
          m_env(new DelayEnv(default_env()))
    {
        // Use the default allocator for these tests: the debug allocator generates a ton of
        // contention, causing the tests to timeout sometimes.
        EXPECT_OK(configure(kRestoreAllocator, nullptr));
        remove_calicodb_files(m_filename);
    }

    ~ConcurrencyTests() override
    {
        EXPECT_OK(configure(kReplaceAllocator, DebugAllocator::config()));
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
        proto.options.create_if_missing = true;

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
                run_consistency_test_instance({
                    param.num_readers,
                    param.num_writers,
                    param.num_checkpointers,
                    i,
                    j,
                    (i & 1) == 0,
                });
                highest_wait_count = maxval(highest_wait_count, s_busy_handler.counter.load(std::memory_order_relaxed));
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
                BucketPtr b;
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
                BucketPtr b;
                auto t = test_create_bucket_if_missing(tx, "b", b);
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
        proto.options.create_if_missing = true;

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
                    run_checkpointer_test_instance({
                        param.num_readers,
                        param.num_writers,
                        param.auto_checkpoint,
                        num_iterations,
                        num_records,
                        checkpoint_mode,
                    });
                    highest_wait_count = maxval(highest_wait_count, s_busy_handler.counter.load(std::memory_order_relaxed));
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
        proto.options.create_if_missing = true;

        std::vector<Connection> connections;
        proto.op = [](auto &co, auto *) {
            auto s = reopen_connection(co);
            s = co.db->update([&co](auto &tx) {
                BucketPtr b;
                auto s = test_create_bucket_if_missing(tx, "b", b);
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
                for (size_t busy_wait = 0; busy_wait <= 1; ++busy_wait) {
                    run_single_writer_test_instance({param.num_readers,
                                                     param.auto_checkpoint,
                                                     num_iterations,
                                                     num_records});
                    highest_wait_count = maxval(highest_wait_count, s_busy_handler.counter.load(std::memory_order_relaxed));
                }
            }
        }
        TEST_LOG << "Highest wait count = " << highest_wait_count << '\n';
    }

    auto run_destruction_test(size_t num_connections) -> void
    {
        TEST_LOG << "ConcurrencyTests.Destruction*\n";
        remove_calicodb_files(m_filename);
        s_busy_handler.counter = 0;

        Connection proto;
        proto.filename = m_filename.c_str();
        proto.env = m_env.get();

        std::vector<Connection> connections;
        proto.options.create_if_missing = false;
        proto.op = [](auto &co, auto *) {
            auto s = reopen_connection(co);
            if (s.is_ok()) {
                // Take some locks and check for bucket "b", which should exist for as long
                // as the database exists.
                s = co.db->view([](const auto &tx) {
                    BucketPtr b;
                    return test_open_bucket(tx, "b", b);
                });
                if (s.is_ok()) {
                    s = co.db->update([](auto &tx) {
                        BucketPtr b;
                        return test_open_bucket(tx, "b", b);
                    });
                    if (s.is_busy()) {
                        s = Status::ok();
                    }
                }
            } else if (s.is_invalid_argument()) {
                // Database no longer exists. Finished.
                co.op = nullptr;
                s = Status::ok();
            }

            delete co.db;
            return s;
        };
        for (size_t i = 0; i < num_connections; ++i) {
            connections.push_back(proto);
        }

        DBPtr db;
        proto.options.create_if_missing = true;
        ASSERT_OK(test_open_db(proto.options, m_filename, db));
        ASSERT_OK(db->update([](auto &tx) {
            BucketPtr b;
            return test_create_bucket(tx, "b", b);
        }));
        db.reset();

        std::vector<std::thread> threads;
        threads.reserve(connections.size());
        for (auto &co : connections) {
            threads.emplace_back([&co] {
                while (connection_main(co, nullptr)) {
                    // Run until the connection clears its own callback.
                }
            });
        }

        proto.env->sleep(1'000);

        Status s;
        do {
            s = DB::destroy(proto.options, m_filename.c_str());
        } while (s.is_busy());
        ASSERT_OK(s);

        for (auto &t : threads) {
            t.join();
        }
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

TEST_F(ConcurrencyTests, Destruction)
{
    run_destruction_test(0);
    run_destruction_test(1);
    run_destruction_test(5);
}

class MultiConnectionTests : public testing::TestWithParam<std::tuple<size_t, size_t>>
{
public:
    const size_t m_num_threads;
    const size_t m_num_processes;
    const std::string m_filename;
    std::vector<std::string> m_messages;

    explicit MultiConnectionTests()
        : m_num_threads(std::get<0>(GetParam())),
          m_num_processes(std::get<1>(GetParam())),
          m_filename(testing::TempDir() + "calicodb_multi_connection_tests")
    {
        remove_calicodb_files(m_filename);
    }

    ~MultiConnectionTests() override = default;

    struct InstanceInfo {
        size_t id;
        int pipe;
    };

    template <class Test>
    auto run_test_instance(const Test &test)
    {
        std::vector<int> std_pipes(m_num_processes); // Pipes for STDOUT and STDERR
        std::vector<int> io_pipes(m_num_processes);  // Pipes for data
        for (size_t n = 0; n < m_num_processes; ++n) {
            int std_pipefd[2];
            ASSERT_EQ(pipe(std_pipefd), 0);
            std_pipes[n] = std_pipefd[0];

            int io_pipefd[2];
            ASSERT_EQ(pipe(io_pipefd), 0);
            io_pipes[n] = io_pipefd[0];

            const auto pid = fork();
            ASSERT_NE(-1, pid) << strerror(errno);
            if (pid) {
                close(io_pipefd[1]);
                close(std_pipefd[1]);
            } else {
                close(std_pipefd[0]);
                close(io_pipefd[0]);
                dup2(std_pipefd[1], STDOUT_FILENO);
                dup2(std_pipefd[1], STDERR_FILENO);

                std::vector<std::thread> threads;
                threads.reserve(m_num_threads);
                const auto t0 = n * m_num_threads;
                for (size_t t = 0; t < m_num_threads; ++t) {
                    threads.emplace_back([test, pipe = io_pipefd[1], id = t + t0] {
                        test(InstanceInfo{id, pipe});
                    });
                }
                for (auto &thread : threads) {
                    thread.join();
                }
                std::exit(testing::Test::HasFailure());
            }
        }

        const auto read_whole_file = [](auto fd) {
            std::string msg;
            for (char buf[256];;) {
                if (const auto rc = read(fd, buf, sizeof(buf))) {
                    EXPECT_GT(rc, 0) << strerror(errno);
                    msg.append(buf, static_cast<size_t>(rc));
                } else {
                    break;
                }
            }
            return msg;
        };
        for (size_t n = 0; n < m_num_processes; ++n) {
            int s;
            const auto pid = wait(&s);
            ASSERT_NE(pid, -1)
                << "wait failed: " << strerror(errno);
            if (!WIFEXITED(s) || WEXITSTATUS(s)) {
                ADD_FAILURE()
                    << "exited " << (WIFEXITED(s) ? "" : "ab")
                    << "normally with exit status " << WEXITSTATUS(s)
                    << "\nOUTPUT:\n"
                    << read_whole_file(std_pipes[n]) << "\n";
            }
        }
        m_messages.resize(m_num_processes);
        for (size_t t = 0; t < m_num_processes; ++t) {
            m_messages[t] = read_whole_file(io_pipes[t]);
        }
    }

    static auto open_database(const Options *options, const std::string &filename) -> DBPtr
    {
        DBPtr db;
        auto opt = options ? *options : Options();
        opt.create_if_missing = true;
        EXPECT_OK(test_open_db(opt, filename, db));
        return db;
    }

    static auto write_data(Bucket &b, const std::vector<size_t> &keys) -> void
    {
        auto c = test_new_cursor(b);
        for (auto k : keys) {
            c->find(numeric_key(k));
            if (c->is_valid()) {
                const auto v = std::stoull(c->value().to_string());
                ASSERT_OK(b.put(*c, numeric_key(v + 1)));
            } else {
                ASSERT_OK(b.put(numeric_key(k), numeric_key(0)));
            }
        }
    }

    static auto write_data(DB &db, const std::vector<size_t> &keys) -> void
    {
        Status s;
        do {
            s = db.update([&keys](auto &tx) {
                write_data(tx.main_bucket(), keys);
                return Status::ok();
            });
        } while (s.is_busy());
        EXPECT_OK(s);
    }

    static auto read_data(const Bucket &b) -> std::vector<ssize_t>
    {
        std::vector<ssize_t> data;
        auto c = test_new_cursor(b);
        // Read all records in the main bucket.
        c->seek_first();
        while (c->is_valid()) {
            const auto k = std::stoull(c->key().to_string());
            if (data.size() <= k) {
                data.resize(k + 1, -1);
            }
            data[k] = std::stoll(c->value().to_string());
            EXPECT_GE(data[k], 0);
            c->next();
        }
        // Repeat the read.
        c->seek_first();
        for (size_t i = 0; i < data.size(); ++i) {
            if (data[i] >= 0) {
                EXPECT_EQ(c->key(), numeric_key(i));
                EXPECT_EQ(c->value().to_string(), numeric_key(static_cast<size_t>(data[i])));
                c->next();
            }
        }
        return data;
    }

    static auto read_data(const DB &db) -> std::vector<ssize_t>
    {
        Status s;
        std::vector<ssize_t> data;
        do {
            s = db.view([&data](const auto &tx) {
                data = read_data(tx.main_bucket());
                return Status::ok();
            });
        } while (s.is_busy());
        EXPECT_OK(s);
        return data;
    }

    static auto expect_identical_values(const Bucket &b, const std::vector<size_t> &keys)
    {
        const auto values = read_data(b);
        std::set<size_t> set(begin(keys), end(keys));
        for (size_t k = 0; k < values.size(); ++k) {
            if (set.find(k) != end(set)) {
                ASSERT_EQ(values[k], values.front());
            }
        }
    }

    static auto expect_identical_values(const std::vector<size_t> &keys, const std::vector<ssize_t> &values)
    {
        if (values.empty()) {
            return;
        }
        for (auto k : keys) {
            // May run before the value is written, so make sure to check bounds.
            if (values.size() > k) {
                ASSERT_EQ(values[k], values[keys.front()]);
            }
        }
    }

    static auto expect_identical_values(const DB &db, const std::vector<size_t> &keys)
    {
        expect_identical_values(keys, read_data(db));
    }

    static auto run_checkpoint(DB &db, CheckpointMode mode = kCheckpointRestart) -> void
    {
        Status s;
        do {
            s = db.checkpoint(mode, nullptr);
        } while (s.is_busy());
        EXPECT_OK(s);
    }

    template <class Callback>
    static auto run_for(ssize_t millis_to_run, Callback &&cb) -> void
    {
        using namespace std::chrono;
        using milli = milliseconds;
        const auto t0 = system_clock::now();
        for (auto t1 = t0;
             duration_cast<milli>(t1 - t0) <= milli(millis_to_run);
             t1 = system_clock::now()) {
            cb();
        }
    }
};

TEST_P(MultiConnectionTests, SanityCheck)
{
    // Each connection updates a different record.
    run_test_instance([this](auto info) {
        auto db = open_database(nullptr, m_filename);
        run_for(3'000, [&] {
            const auto id = info.id;
            const auto before = read_data(*db);
            write_data(*db, {id});
            const auto after = read_data(*db);
            ASSERT_GT(after.size(), id);
            if (id < before.size()) {
                ASSERT_EQ(before[id], after[id] - 1);
            } else {
                ASSERT_EQ(after[id], 0);
            }
        });
    });
}

static const std::vector<size_t> s_all_keys = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                                               17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31};

TEST_P(MultiConnectionTests, Atomicity1)
{
    TEST_LOG << "MultiConnectionTests.Atomicity1\n";
    run_test_instance([this](auto info) {
        auto db = open_database(nullptr, m_filename);
        run_for(3'000, [&] {
            if (info.id) {
                // Other connections check for consistency.
                expect_identical_values(*db, s_all_keys);
            } else {
                // First connection updates the same records each time.
                write_data(*db, s_all_keys);
            }
        });
    });

    const auto db = open_database(nullptr, m_filename);
    const auto values = read_data(*db);
    expect_identical_values(s_all_keys, values);
    TEST_LOG << "Number of updates: " << values.front();
    ASSERT_GT(values.front(), 0);
}

static const std::vector<size_t> s_half_keys1 = {0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30};
static const std::vector<size_t> s_half_keys2 = {1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31};

TEST_P(MultiConnectionTests, Atomicity2)
{
    TEST_LOG << "MultiConnectionTests.Atomicity2\n";
    run_test_instance([this](auto info) {
        auto db = open_database(nullptr, m_filename);
        run_for(3'000, [&] {
            const auto id = info.id;
            if (id & 1) {
                write_data(*db, s_half_keys2);
                expect_identical_values(*db, s_half_keys1);
            } else {
                // id 0 must write key 0.
                write_data(*db, s_half_keys1);
                expect_identical_values(*db, s_half_keys2);
            }
        });
    });

    const auto db = open_database(nullptr, m_filename);
    const auto values = read_data(*db);
    expect_identical_values(s_half_keys1, values);
    expect_identical_values(s_half_keys2, values);
    TEST_LOG << "Number of updates: " << values.front() << '\n';
    ASSERT_GT(values.front(), 0);
}

INSTANTIATE_TEST_SUITE_P(
    SanityCheck,
    MultiConnectionTests,
    testing::Values(std::make_tuple(1, 1))); // Single connection

INSTANTIATE_TEST_SUITE_P(
    RunTests,
    MultiConnectionTests,
    testing::Combine(
        testing::Values(2, 10),  // Number of threads
        testing::Values(2, 5))); // Number of processes

} // namespace calicodb::test