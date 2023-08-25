// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/db.h"
#include "common.h"
#include "logging.h"
#include "test.h"
#include <condition_variable>
#include <filesystem>
#include <iomanip>
#include <thread>

namespace calicodb::test
{

// Counting semaphore
class Semaphore
{
    mutable std::mutex m_mu;
    std::condition_variable m_cv;
    size_t m_available;

public:
    explicit Semaphore(size_t n = 0)
        : m_available(n)
    {
    }

    Semaphore(Semaphore &) = delete;
    void operator=(Semaphore &) = delete;

    auto wait() -> void
    {
        std::unique_lock lock(m_mu);
        m_cv.wait(lock, [this] {
            return m_available > 0;
        });
        --m_available;
    }

    auto signal(size_t n = 1) -> void
    {
        m_mu.lock();
        m_available += n;
        m_mu.unlock();
        m_cv.notify_all();
    }
};

// Reusable thread barrier
class Barrier
{
    Semaphore m_phase_1;
    Semaphore m_phase_2;
    mutable std::mutex m_mu;
    const size_t m_max_count;
    size_t m_count = 0;

public:
    explicit Barrier(size_t max_count)
        : m_max_count(max_count)
    {
    }

    Barrier(Barrier &) = delete;
    void operator=(Barrier &) = delete;

    // Wait for m_max_count threads to call this routine
    auto wait() -> void
    {
        m_mu.lock();
        if (++m_count == m_max_count) {
            m_phase_1.signal(m_max_count);
        }
        m_mu.unlock();
        m_phase_1.wait();

        m_mu.lock();
        if (--m_count == 0) {
            m_phase_2.signal(m_max_count);
        }
        m_mu.unlock();
        m_phase_2.wait();
    }
};

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
    DelayEnv *m_env;

    explicit ConcurrencyTests()
        : m_filename(testing::TempDir() + "concurrency"),
          m_env(new DelayEnv(Env::default_env()))
    {
        std::filesystem::remove_all(m_filename);
        std::filesystem::remove_all(m_filename + kDefaultWalSuffix.to_string());
        std::filesystem::remove_all(m_filename + kDefaultShmSuffix.to_string());
    }

    ~ConcurrencyTests() override
    {
        delete m_env;
    }

    auto TearDown() -> void override
    {
        // All resources should have been cleaned up by now, even though the Env is not deleted.
        ASSERT_EQ(Alloc::bytes_used(), 0) << "leaked " << std::setprecision(4)
                                          << static_cast<double>(Alloc::bytes_used()) / (1'024.0 * 1'024)
                                          << " MiB";
    }

    struct Connection;
    using Operation = std::function<Status(Connection &, Barrier *)>;

    struct Connection {
        Operation op;
        size_t op_args[4] = {};
        const char *filename = nullptr;
        BusyHandler *busy = nullptr;
        Env *env = nullptr;

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

        // These parameters should not be set manually. run_test() will iterate over various
        // combinations of them.
        size_t num_iterations = 0;
        size_t num_records = 0;
        bool checkpoint_reset = false;
        bool delay_barrier = false;
        bool delay_sync = false;
    };
    auto run_test_instance(const ConsistencyTestParameters &param) -> void
    {
        (void)DB::destroy(Options(), m_filename.c_str());

        const auto nt = param.num_readers + param.num_writers + param.num_checkpointers;
        Barrier barrier(nt);

        Connection tmp;
        tmp.filename = m_filename.c_str();
        tmp.env = m_env;
        tmp.op_args[0] = param.num_iterations;
        tmp.op_args[1] = param.num_records;

        std::vector<Connection> connections;
        tmp.op = test_reader;
        for (size_t i = 0; i < param.num_readers; ++i) {
            connections.emplace_back(tmp);
        }
        tmp.op = test_writer;
        for (size_t i = 0; i < param.num_writers; ++i) {
            connections.emplace_back(tmp);
        }
        // Write some records to the WAL.
        ASSERT_OK(test_writer(tmp, nullptr));

        tmp.op = test_checkpointer;
        tmp.op_args[1] = param.checkpoint_reset;
        for (size_t i = 0; i < param.num_checkpointers; ++i) {
            connections.emplace_back(tmp);
        }
        // Write the WAL back to the database. If `param.checkpoint_reset` is true, then the WAL will be
        // reset such that writers start at the beginning again.
        ASSERT_OK(test_checkpointer(tmp, nullptr));

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
            // Check the results (only readers output anything).
            for (size_t i = 0; i + 1 < co.result.size(); ++i) {
                uint64_t n;
                Slice slice(co.result[i]);
                ASSERT_LE(slice, co.result[i + 1]);
                ASSERT_TRUE(consume_decimal_number(slice, &n));
                ASSERT_TRUE(slice.is_empty());
            }
        }
    }
    auto run_test(const ConsistencyTestParameters &param) -> void
    {
        for (size_t i = 1; i <= 4; ++i) {
            for (size_t j = 1; j <= 4; ++j) {
                for (size_t k = 1; k <= 4; ++k) {
                    run_test_instance({
                        param.num_readers,
                        param.num_writers,
                        param.num_checkpointers,
                        i,
                        j,
                        (i & 1) == 0,
                        (j & 1) == 0,
                        (k & 1) == 0,
                    });
                }
            }
        }
    }

    [[nodiscard]] static auto reopen_connection(Connection &co, const Options *options) -> Status
    {
        static class AlwaysWait : public BusyHandler
        {
        public:
            ~AlwaysWait() override = default;
            [[nodiscard]] auto exec(unsigned) -> bool override
            {
                return true;
            }
        } s_busy_handler;

        auto opt = options ? *options : Options();
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
    // 1. If the bucket named "BUCKET" exists, it contains co.op_arg records
    // 2. Record keys are monotonically increasing integers starting from 0, serialized
    //    using numeric_key()
    // 3. Each record value is another such serialized integer, however, each value is
    //    identical
    // 4. The record value read by a reader must never decrease between runs
    [[nodiscard]] static auto test_reader(Connection &co, Barrier *barrier) -> Status
    {
        barrier_wait(barrier);

        Options options;
        options.create_if_missing = false;
        auto s = reopen_connection(co, &options);
        for (size_t n = 0; s.is_ok() && n < co.op_args[0]; ++n) {
            barrier_wait(barrier);

            s = co.db->run(ReadOptions(), [&co, barrier](const auto &tx) {
                barrier_wait(barrier);

                TestCursor c;
                // Oddly enough, if we name this Status "s", some platforms complain about shadowing,
                // even though s is not captured in this lambda.
                auto t = test_open_bucket(tx, "BUCKET", c);
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
                    c->find(numeric_key(i % co.op_args[1]));
                    if (!c->is_valid()) {
                        t = c->status();
                        break;
                    } else if (i == 0) {
                        const auto value = c->value();
                        co.result.emplace_back(value.data(), value.size());
                    } else {
                        EXPECT_EQ(c->value(), co.result.back()) << "non repeatable read";
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
    // either creates or increases co.op_arg[1] records in a bucket named "BUCKET". The
    // first writer to run creates the bucket.
    [[nodiscard]] static auto test_writer(Connection &co, Barrier *barrier) -> Status
    {
        barrier_wait(barrier);

        auto s = reopen_connection(co, nullptr);
        for (size_t n = 0; s.is_ok() && n < co.op_args[0]; ++n) {
            barrier_wait(barrier);

            s = co.db->run(WriteOptions(), [&co, barrier](auto &tx) {
                barrier_wait(barrier);

                TestCursor c;
                auto t = test_create_and_open_bucket(tx, BucketOptions(), "BUCKET", c);
                for (size_t i = 0; t.is_ok() && i < co.op_args[1]; ++i) {
                    uint64_t result = 1;
                    c->find(numeric_key(i));
                    if (c->is_valid()) {
                        Slice slice(c->value());
                        EXPECT_TRUE(consume_decimal_number(slice, &result));
                        ++result;
                    } else if (c->status().is_not_found()) {
                        t = Status::ok();
                    } else {
                        break;
                    }
                    if (t.is_ok()) {
                        t = tx.put(*c, numeric_key(i), numeric_key(result));
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

        Options options;
        options.create_if_missing = false;
        auto s = reopen_connection(co, &options);
        for (size_t n = 0; s.is_ok() && n < co.op_args[0]; ++n) {
            barrier_wait(barrier);
            barrier_wait(barrier);
            s = co.db->checkpoint(co.op_args[1]);

            if (s.is_busy()) {
                s = Status::ok();
            }
        }

        barrier_wait(barrier);
        co.op = nullptr;
        delete co.db;
        return s;
    }

    struct AllocTestParameters {
        size_t num_malloc;
        size_t num_realloc;
        size_t max_alloc;
        size_t limit;
    };
    auto run_alloc_test_instance(const AllocTestParameters &param) -> void
    {
        static constexpr size_t kNumIterations = 2; //'000;
        ASSERT_EQ(Alloc::bytes_used(), 0);
        ASSERT_EQ(Alloc::set_limit(param.limit), 0);

        const auto nt = param.num_malloc + param.num_realloc;
        Barrier barrier(nt);

        Connection tmp;
        tmp.filename = m_filename.c_str();
        tmp.env = m_env;

        struct AllocState {
            char *ptr;
            unsigned state;
        };
        std::vector<Connection> connections;
        std::vector<AllocState> malloc_states(param.num_malloc);
        for (auto &state : malloc_states) {
            const uint64_t min_alloc = param.limit == 1;
            tmp.op_args[0] = m_random.Next(min_alloc, param.max_alloc);
            tmp.op = [&state](auto &co, auto *b) {
                barrier_wait(b);
                if (state.ptr) {
                    Alloc::free(state.ptr);
                    state.ptr = nullptr;
                } else {
                    state.ptr = static_cast<char *>(Alloc::malloc(co.op_args[0]));
                }
                if (state.state++ > kNumIterations) {
                    co.op = nullptr;
                }
                return Status::ok();
            };
            connections.emplace_back(tmp);
        }
        std::vector<AllocState> realloc_states(param.num_realloc);
        for (auto &state : realloc_states) {
            tmp.op_args[0] = m_random.Next(param.max_alloc);
            tmp.op_args[1] = m_random.Next(1, param.max_alloc);
            tmp.op = [&state](auto &co, auto *b) {
                barrier_wait(b);
                size_t new_size;
                switch (state.state & 0b11) {
                    case 0b00:
                        new_size = 0;
                        break;
                    case 0b01:
                        new_size = co.op_args[0];
                        break;
                    case 0b10:
                        new_size = co.op_args[1];
                        break;
                    default:
                        new_size = co.op_args[0] + co.op_args[1];
                }
                auto *new_ptr = Alloc::realloc(state.ptr, new_size);
                if (new_ptr || new_size == 0) {
                    state.ptr = static_cast<char *>(new_ptr);
                }
                if (state.state++ > kNumIterations) {
                    co.op = nullptr;
                }
                return Status::ok();
            };
            connections.emplace_back(tmp);
        }

        std::vector<std::thread> threads;
        threads.reserve(connections.size());
        for (auto &co : connections) {
            threads.emplace_back([&co, b = &barrier, t = threads.size(), limit = param.limit] {
                while (connection_main(co, b)) {
                    ASSERT_LE(Alloc::bytes_used(), limit);
                }
            });
        }
        for (auto &t : threads) {
            t.join();
        }

        for (const auto &[ptr, _] : malloc_states) {
            Alloc::free(ptr);
        }
        for (const auto &[ptr, _] : realloc_states) {
            Alloc::free(ptr);
        }

        ASSERT_EQ(Alloc::bytes_used(), 0);
    }

    auto run_alloc_test(size_t x, size_t y) -> void
    {
        for (size_t limit : {10, 100, 1'000}) {
            for (size_t max_alloc = 1; max_alloc <= limit; max_alloc *= 2) {
                run_alloc_test_instance({x, y, max_alloc, limit});
                if (x != y) {
                    run_alloc_test_instance({y, x, max_alloc, limit});
                }
            }
        }
    }
};

TEST_F(ConcurrencyTests, Database0)
{
    // Sanity check, no concurrency.
    run_test({1, 0, 0});
    run_test({0, 1, 0});
    run_test({0, 0, 1});
}

TEST_F(ConcurrencyTests, Database1)
{
    run_test({10, 0, 0});
    run_test({0, 10, 0});
    run_test({0, 0, 10});
}

TEST_F(ConcurrencyTests, Database2)
{
    run_test({10, 0, 1});
    run_test({10, 1, 0});
    run_test({10, 1, 1});
    run_test({0, 10, 1});
    run_test({1, 10, 0});
    run_test({1, 10, 1});
    run_test({0, 1, 10});
    run_test({1, 0, 10});
    run_test({1, 1, 10});
}

TEST_F(ConcurrencyTests, Database3)
{
    run_test({100, 10, 10});
    run_test({10, 100, 10});
    run_test({10, 10, 100});
}

TEST_F(ConcurrencyTests, Database4)
{
    run_test({50, 50, 50});
}

TEST_F(ConcurrencyTests, Allocation0)
{
    // Sanity check, no concurrency.
    run_alloc_test(0, 1);
}

TEST_F(ConcurrencyTests, Allocation1)
{
    run_alloc_test(0, 10);
}

TEST_F(ConcurrencyTests, Allocation2)
{
    run_alloc_test(1, 10);
}

TEST_F(ConcurrencyTests, Allocation3)
{
    run_alloc_test(10, 100);
}

TEST_F(ConcurrencyTests, Allocation4)
{
    run_alloc_test(50, 50);
}

} // namespace calicodb::test