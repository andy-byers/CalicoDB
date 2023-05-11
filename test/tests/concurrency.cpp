// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "tools.h"
#include <gtest/gtest.h>
#include <thread>

namespace calicodb::test::concurrency
{

struct TestState {
    std::size_t pid = 0;
    std::size_t tid = 0;
    Env *env = nullptr;
};
using TestRoutine = std::function<void(TestState)>;
class TestHarness
{
public:
    explicit TestHarness(Env &env)
        : m_env(&env)
    {
    }

    virtual ~TestHarness()
    {
        delete m_env;
    }

    virtual auto register_routine(bool bkgd, TestRoutine routine) -> void
    {
        if (bkgd) {
            m_bkgd.emplace_back(std::move(routine));
        } else {
            m_main.emplace_back(std::move(routine));
        }
    }

    virtual auto test(std::size_t num_processes) -> void
    {
        ASSERT_LT(0, num_processes) << "incorrect test parameters";
        for (std::size_t n = 1; n < num_processes; ++n) {
            const auto pid = fork();
            if (pid == 0) {
                // Run the n'th batch of callbacks registered to a child process.
                run_process(n, m_bkgd);
                std::exit(testing::Test::HasFailure());
            } else if (pid < 0) {
                ADD_FAILURE() << "fork(): " << strerror(errno);
            }
        }
        // Run the batch of callbacks registered to the parent process. Blocks until all threads have
        // joined.
        run_process(0, m_main);

        // Wait on the child processes to finish.
        struct Result {
            pid_t pid;
            int s;
        };
        std::vector<Result> result(num_processes - 1);
        for (auto &r : result) {
            r.pid = wait(&r.s);
        }
        for (auto [pid, s] : result) {
            ASSERT_NE(pid, -1)
                << "wait(): " << strerror(errno);
            ASSERT_TRUE(WIFEXITED(s) && WEXITSTATUS(s) == 0)
                << "exited " << (WIFEXITED(s) ? "" : "ab")
                << "normally with exit status "
                << WEXITSTATUS(s);
        }
    }

protected:
    auto run_process(std::size_t pid, const std::vector<TestRoutine> &routines) -> void
    {
        std::vector<std::thread> threads;
        std::atomic<bool> start(false);
        for (const auto &routine : routines) {
            const auto tid = threads.size();
            threads.emplace_back([pid, &routine, &start, tid, this] {
                while (!start.load(std::memory_order_acquire)) {
                }
                routine({pid, tid, m_env});
            });
        }
        start.store(true, std::memory_order_release);
        for (auto &thread : threads) {
            thread.join();
        }
    }

    std::vector<TestRoutine> m_main;
    std::vector<TestRoutine> m_bkgd;
    Env *m_env;
};

static constexpr const char *kDBName = "concurrencyDB";

// Keep track of an unsigned integer that is only allowed to increase
// This class ensures that `check_tracker()` is called at least `m_interval` times before the
// stored value changes. The stored value, `m_number` is only allowed to increase.
class Tracker
{
protected:
    explicit Tracker(std::size_t num_rounds)
        : m_interval(num_rounds)
    {
    }

    auto check_tracker(std::size_t number) -> void
    {
        if (m_round == 0) {
            ASSERT_LE(m_number, number);
            m_number = number;
        } else {
            ASSERT_EQ(m_number, number);
        }
        m_round = (m_round + 1) % m_interval;
    }

    std::size_t m_number = 0;

private:
    const std::size_t m_interval;
    std::size_t m_round = 0;
};

// Concurrency test writer routine
// The first writer to run will create a table named `tbname` and insert `count` records. At first,
// each record value is identical. Each subsequent writer iterates through the records and increases
// each value by 1.
class WriterRoutine
    : public TxnHandler,
      private Tracker
{
    const std::string m_tbname;
    const std::size_t m_count;

public:
    explicit WriterRoutine(std::string tbname, std::size_t count)
        : Tracker(count),
          m_tbname(std::move(tbname)),
          m_count(count)
    {
    }

    ~WriterRoutine() override = default;

    [[nodiscard]] auto exec(Txn &txn) -> Status override
    {
        Table *table;
        CALICODB_TRY(txn.new_table(TableOptions(), m_tbname, table));
        for (std::size_t i = 0; i < m_count; ++i) {
            std::string value;
            auto s = table->get(tools::integral_key(i), &value);
            if (s.is_not_found()) {
                value = "0";
            } else if (!s.is_ok()) {
                return s;
            }
            auto number = tools::NumericKey(value);
            check_tracker(number.number());
            ++number;
            CALICODB_TRY(table->put(tools::integral_key(i), number.string()));
        }
        delete table;
        // Commit the transaction.
        return Status::ok();
    }
};

// Concurrency test reader routine
// Reader instances spin until a writer creates and populates the table `tbname` with `count` records.
// Readers read through each record and (a) make sure that each value is the same, and (b) make sure that
// the record value is greater than or equal to the record value encountered on the last round.
class ReaderRoutine
    : public TxnHandler,
      private Tracker
{
    const std::string m_tbname;
    const std::size_t m_count;

public:
    explicit ReaderRoutine(std::string tbname, std::size_t count)
        : Tracker(count),
          m_tbname(std::move(tbname)),
          m_count(count)
    {
    }

    ~ReaderRoutine() override = default;

    [[nodiscard]] auto exec(Txn &txn) -> Status override
    {
        Table *table;
        auto s = txn.new_table(TableOptions(), m_tbname, table);
        if (s.is_invalid_argument()) {
            // Writer hasn't created the table yet.
            return Status::ok();
        } else if (!s.is_ok()) {
            return s;
        }
        for (std::size_t i = 0; i < m_count; ++i) {
            std::string value;
            // If the table exists, then it must contain `m_count` records (the first writer to run
            // makes sure of this).
            CALICODB_TRY(table->get(tools::integral_key(i), &value));
            check_tracker(tools::NumericKey(value).number());
        }
        delete table;
        return Status::ok();
    }
};

using ConcurrencyTestParam = std::tuple<
    std::size_t,
    std::size_t,
    std::size_t>;
class ConcurrencyTests
    : public testing::TestWithParam<ConcurrencyTestParam>,
      public TestHarness
{
protected:
    const std::size_t kNumWriters = std::get<0>(GetParam());
    const std::size_t kNumReaders = std::get<1>(GetParam());
    const std::size_t kExtraInfo = std::get<2>(GetParam());
    const std::size_t kNumRecords = 1'000;
    const std::size_t kNumRounds = 100;

    explicit ConcurrencyTests()
        : TestHarness(*Env::default_env())
    {
        (void)DB::destroy(Options(), kDBName);
    }

    ~ConcurrencyTests() override = default;

    auto test(std::size_t num_processes) -> void override
    {
        for (std::size_t i = 0; i < num_processes; ++i) {
            for (std::size_t r = 0; r < kNumReaders; ++r) {
                register_routine(i > 0, [this](auto st) {
                    return run_reader_routine(st);
                });
            }
            for (std::size_t w = 0; w < kNumWriters; ++w) {
                register_routine(i > 0, [this](auto st) {
                    return run_writer_routine(st);
                });
            }
        }
        TestHarness::test(num_processes);
    }

    auto run_reader_routine(TestState st) -> void
    {
        ReaderRoutine reader("TABLE", kNumRecords);
        auto reopen = true;
        DB *db = nullptr;

        for (std::size_t i = 0; i < kNumRounds;) {
            Status s;

            auto is_open = true;
            if (reopen) {
                delete db;
                db = nullptr;

                Options dbopt;
                dbopt.busy = &m_busy;
                dbopt.create_if_missing = false;
                s = DB::open(dbopt, kDBName, db);
                reopen = kExtraInfo;
                is_open = s.is_ok();
            }
            if (is_open) {
                while ((s = db->view(reader)).is_busy()) {
                    // May get a "busy" error if another connection is resetting the log.
                }
            } else if (s.is_invalid_argument()) {
                // Forgive readers that couldn't create the file.
                reopen = true;
                continue;
            }
            EXPECT_TRUE(s.is_ok())
                << "reader " << st.pid << ':' << st.tid << " (PID:TID) failed on `DB::" << (is_open ? "view" : "open")
                << "()` with \"" << s.to_string();
            ++i;
        }
        delete db;
    }
    auto run_writer_routine(TestState st) -> void
    {
        WriterRoutine writer("TABLE", kNumRecords);
        auto reopen = true;
        DB *db = nullptr;

        for (std::size_t i = 0; i < kNumRounds; ++i) {
            Status s;

            auto is_open = true;
            if (reopen) {
                delete db;
                db = nullptr;

                Options dbopt;
                dbopt.busy = &m_busy;
                s = DB::open(dbopt, kDBName, db);
                reopen = kExtraInfo;
                is_open = s.is_ok();
            }
            if (is_open) {
                while ((s = db->update(writer)).is_busy()) {
                    // May get a "busy" error if another connection is resetting the log.
                }
            }
            EXPECT_TRUE(s.is_ok())
                << "writer " << st.pid << ':' << st.tid << " (PID:TID) failed on `DB::" << (is_open ? "update" : "open")
                << "()` with \"" << s.to_string();
        }
        delete db;
    }

    TestRoutine m_writer;
    TestRoutine m_reader;
    tools::BusyCounter m_busy;
};

TEST_P(ConcurrencyTests, MT)
{
    test(1);
}

TEST_P(ConcurrencyTests, MP_2)
{
    test(2);
}

TEST_P(ConcurrencyTests, MP_3)
{
    test(3);
}

INSTANTIATE_TEST_CASE_P(
    ConcurrencyTests,
    ConcurrencyTests,
    testing::Combine(
        testing::Values(1, 2, 10),
        testing::Values(1, 2, 10, 100),
        testing::Values(0)),
    [](const auto &info) {
        std::string label;
        append_number(label, std::get<0>(info.param));
        label.append("Writer");
        if (std::get<0>(info.param) > 1) {
            label += 's';
        }
        label += '_';
        append_number(label, std::get<1>(info.param));
        label.append("Reader");
        if (std::get<1>(info.param) > 1) {
            label += 's';
        }
        if (std::get<2>(info.param)) {
            label.append("_Reopen");
        }
        return label;
    });

} // namespace calicodb::test::concurrency