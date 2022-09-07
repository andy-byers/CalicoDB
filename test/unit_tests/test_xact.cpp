
#include "calico/bytes.h"
#include "calico/options.h"
#include "calico/storage.h"
#include "core/core.h"
#include "fakes.h"
#include "pager/basic_pager.h"
#include "tools.h"
#include "tree/tree.h"
#include "unit_tests.h"

namespace calico {

namespace fs = std::filesystem;

namespace internal {
    extern std::uint32_t random_seed;
} // namespace internal

namespace interceptors {
    extern OpenInterceptor open;
    extern ReadInterceptor read;
    extern WriteInterceptor write;
    extern SyncInterceptor sync;
} // namespace interceptors

class XactTests: public TestOnDisk {
public:
    auto SetUp() -> void override
    {
        options.page_size = 0x400;
        options.frame_count = 32;
        options.log_level = spdlog::level::trace;
        options.store = store.get();

        ASSERT_TRUE(expose_message(db.open(ROOT, options)));
    }

    auto TearDown() -> void override
    {
        interceptors::reset();
        ASSERT_TRUE(expose_message(db.close()));
    }

    RecordGenerator generator {{16, 100, 10, false, true}};
    Random random {internal::random_seed};
    Options options;
    Core db;
};

TEST_F(XactTests, NewDatabaseIsOk)
{
    ASSERT_TRUE(expose_message(db.status()));
}

template<class Action>
static auto with_xact(XactTests &test, const Action &action)
{
    auto xact = test.db.transaction();
    action();
    ASSERT_TRUE(expose_message(xact.commit()));
}

template<class Test>
static auto insert_1000_records(Test &test)
{
    auto records = test.generator.generate(test.random, 1'000);
    for (const auto &r: records) {
        EXPECT_TRUE(expose_message(test.db.insert(stob(r.key), stob(r.value))));
    }
    return records;
}

auto erase_1000_records(XactTests &test)
{
    for (Size i {}; i < 1'000; ++i) {
        ASSERT_TRUE(expose_message(test.db.erase(test.db.first())));
    }
}

TEST_F(XactTests, AbortFirstXact)
{
    auto xact = db.transaction();
    insert_1000_records(*this);
    ASSERT_TRUE(expose_message(xact.abort()));
    ASSERT_EQ(db.info().record_count(), 0);

    // Normal operations after abort should work.
    with_xact(*this, [this] {insert_1000_records(*this);});
}

TEST_F(XactTests, CommitIsACheckpoint)
{
    with_xact(*this, [this] {insert_1000_records(*this);});

    auto xact = db.transaction();
    ASSERT_TRUE(expose_message(xact.abort()));
    ASSERT_EQ(db.info().record_count(), 1'000);
}

TEST_F(XactTests, KeepsCommittedRecords)
{
    with_xact(*this, [this] {insert_1000_records(*this);});

    auto xact = db.transaction();
    erase_1000_records(*this);
    ASSERT_TRUE(expose_message(xact.abort()));
    ASSERT_EQ(db.info().record_count(), 1'000);

    // Normal operations after abort should work.
    with_xact(*this, [this] {erase_1000_records(*this);});
    ASSERT_EQ(db.info().record_count(), 0);
}

template<class Test, class Itr>
auto run_random_operations(Test &test, const Itr &begin, const Itr &end)
{
    for (auto itr = begin; itr != end; ++itr) {
        EXPECT_TRUE(expose_message(test.db.insert(stob(itr->key), stob(itr->value))));
    }

    std::vector<Record> committed;
    for (auto itr = begin; itr != end; ++itr) {
        if (test.random.get(5) == 0) {
            EXPECT_TRUE(expose_message(test.db.erase(stob(itr->key))));
        } else {
            committed.emplace_back(*itr);
        }
    }
    return committed;
}

TEST_F(XactTests, AbortRestoresPriorState)
{
    static constexpr Size NUM_RECORDS {500};
    const auto path = ROOT + std::string {DATA_FILENAME};
    const auto records = generator.generate(random, NUM_RECORDS);

    auto xact = db.transaction();
    auto committed = run_random_operations(*this, cbegin(records), cbegin(records) + NUM_RECORDS/2);
    ASSERT_TRUE(expose_message(xact.commit()));

    xact = db.transaction();
    run_random_operations(*this, cbegin(records) + NUM_RECORDS/2, cend(records));
    ASSERT_TRUE(expose_message(xact.abort()));

    // The database should contain exactly these records.
    ASSERT_EQ(db.info().record_count(), committed.size());
    for (const auto &[key, value]: committed) {
        ASSERT_TRUE(tools::contains(db, key, value));
    }
}

template<class Test>
[[nodiscard]]
auto run_random_transactions(Test &test, Size n)
{
    static constexpr long XACT_SIZE {100};
    // Generate the records all at once, so we know that they are unique.
    auto all_records = test.generator.generate(test.random, n * XACT_SIZE);
    std::vector<Record> committed;

    for (Size i {}; i < n; ++i) {
        auto xact = test.db.transaction();
        const auto start = cbegin(all_records) + static_cast<long>(XACT_SIZE * i);
        const auto temp = run_random_operations(test, start, start + XACT_SIZE);
        if (test.random.get(4) == 0) {
            EXPECT_TRUE(expose_message(xact.abort()));
        } else {
            EXPECT_TRUE(expose_message(xact.commit()));
            committed.insert(cend(committed), cbegin(temp), cend(temp));
        }
    }
    return committed;
}

TEST_F(XactTests, SanityCheck)
{
    for (const auto &[key, value]: run_random_transactions(*this, 20)) {
        ASSERT_TRUE(tools::contains(db, key, value));
    }
}

TEST_F(XactTests, PersistenceSanityCheck)
{
    ASSERT_TRUE(expose_message(db.close()));
    std::vector<Record> committed;

    for (Size i {}; i < 5; ++i) {
        ASSERT_TRUE(expose_message(db.open(ROOT, options)));
        const auto current = run_random_transactions(*this, 10);
        committed.insert(cend(committed), cbegin(current), cend(current));
        ASSERT_TRUE(expose_message(db.close()));
    }

    ASSERT_TRUE(expose_message(db.open(ROOT, options)));
    for (const auto &[key, value]: committed) {
        ASSERT_TRUE(tools::contains(db, key, value));
    }
}

TEST_F(XactTests, AtomicOperationSanityCheck)
{
    const auto all_records = generator.generate(random, 500);
    const auto committed = run_random_operations(*this, cbegin(all_records), cend(all_records));

    for (const auto &[key, value]: committed) {
        ASSERT_TRUE(tools::contains(db, key, value));
    }
}

class FailureTests: public TestOnHeap {
public:
    FailureTests() = default;

    ~FailureTests() override
    {
        interceptors::reset();
    }

    auto SetUp() -> void override
    {
        Options options;
        options.page_size = 0x200;
        options.frame_count = 16;
        options.store = store.get();
        options.log_level = spdlog::level::err; // TODO
        ASSERT_TRUE(expose_message(db.open(ROOT, options)));
    }

    RecordGenerator generator {{16, 100, 10, false, true}};
    Random random {internal::random_seed};
    Database db;
};

auto add_sequential_records(Database &db, Size n)
{
    for (Size i {}; i < n; ++i) {
        const auto key = make_key(i);
        ASSERT_TRUE(expose_message(db.insert(key, key)));
    }
}

auto modify_until_failure(FailureTests &test, Size limit = 10'000) -> Status
{
    RecordGenerator::Parameters param;
    param.mean_key_size = 16;
    param.mean_value_size = 100;
    param.is_unique = true;
    param.spread = 0;
    RecordGenerator generator {param};

    const auto info = test.db.info();
    auto s = Status::ok();

    for (Size i {}; i < limit; ++i) {
        for (const auto &[key, value]: generator.generate(test.random, 100)) {
            // insert()/erase() exercise data file reading/writing, and WAL file writing.
            if (test.random.get(4) == 0 && info.record_count()) {
                s = test.db.erase(test.db.first());
            } else {
                s = test.db.insert(key, value);
            }
            if (!s.is_ok()) return s;
        }
    }
    return Status::ok();
}

template<class Test>
static auto run_propagate_test(Test &test)
{
    // Modify the database until a system call fails.
    auto xact = test.db.transaction();
    const auto s = modify_until_failure(test);
    assert_error_42(s);

    // The database status should reflect the error returned by write().
    assert_error_42(test.db.status());
    (void)xact.abort();
}

TEST_F(FailureTests, DataReadErrorIsPropagatedDuringModify)
{
    interceptors::set_read(FailOnce<5> {"test/data"});
    run_propagate_test(*this);
}

TEST_F(FailureTests, DataWriteErrorIsPropagatedDuringModify)
{
    interceptors::set_write(FailOnce<5> {"test/data"});
    run_propagate_test(*this);
}

// TODO: Occasionally causes a deadlock!!!
TEST_F(FailureTests, WalWriteErrorIsPropagatedDuringModify)
{
    interceptors::set_write(FailOnce<5> {"test/wal-"});
    run_propagate_test(*this);
}

// TODO: Doesn't work!
TEST_F(FailureTests, WalOpenErrorIsPropagatedDuringModify)
{
    interceptors::set_open(FailOnce<1> {"test/wal-"});
    run_propagate_test(*this);
}

TEST_F(FailureTests, WalReadErrorIsPropagatedDuringAbort)
{
    interceptors::set_read(FailOnce<0> {"test/wal-"});

    auto xact = db.transaction();
    insert_1000_records(*this);
    assert_error_42(xact.abort());
    assert_error_42(db.status());
}

TEST_F(FailureTests, DataReadErrorIsNotPropagatedDuringQuery)
{
    add_sequential_records(db, 500);

    interceptors::set_read(FailOnce<5> {"test/data"});

    // Iterate until a read() call fails.
    auto c = db.first();
    for (; c.is_valid(); ++c) {}

    // The error in the cursor should reflect the read() error.
    assert_error_42(c.status());

    // The database status should still be OK. Errors during reads cannot corrupt or even modify the database state.
    ASSERT_TRUE(expose_message(db.status()));
}

TEST_F(FailureTests, DataWriteFailureDuringQuery)
{
    // This tests database behavior when we encounter an error while flushing a dirty page to make room for a page read
    // during a query. In this case, we don't have a transaction we can try to abort, so we must exit the program. Next
    // time the database is opened, it will roll forward and apply any missing updates.
    add_sequential_records(db, 500);

    interceptors::set_write(FailOnce<5> {"test/data"});

    auto c = db.first();
    for (; c.is_valid(); ++c) {}

    assert_error_42(c.status());
    assert_error_42(db.status());
}

TEST_F(FailureTests, DatabaseNeverWritesAfterPagesAreFlushedDuringQuery)
{
    add_sequential_records(db, 500);

    // This will cause all dirty pages to eventually be evicted to make room.
    auto c = db.first();
    for (; c.is_valid(); ++c) {}

    // Writes to any file will fail.
    interceptors::set_write(FailOnce<0> {"test/"});

    // We should be able to iterate through all pages without any writes occurring.
    c = db.first();
    for (; c.is_valid(); ++c) {}

    auto s = c.status();
    ASSERT_TRUE(s.is_not_found()) << s.what();

    s = db.status();
    ASSERT_TRUE(s.is_ok()) << s.what();
}

template<class Test>
static auto run_abort_restores_state_test(Test &test) -> void
{
    auto xact = test.db.transaction();
    auto s = modify_until_failure(test);
    assert_error_42(s);

    s = test.db.status();
    assert_error_42(s);

    ASSERT_TRUE(expose_message(xact.abort()));
    ASSERT_TRUE(expose_message(test.db.status()));
}

TEST_F(FailureTests, AbortRestoresStateAfterDataReadError)
{
    interceptors::set_read(FailOnce<5> {"test/data"});
    run_abort_restores_state_test(*this);
}

TEST_F(FailureTests, AbortRestoresStateAfterDataWriteError)
{
    interceptors::set_write(FailOnce<5> {"test/data"});
    run_abort_restores_state_test(*this);
}

TEST_F(FailureTests, AbortRestoresStateAfterWalWriteError)
{
    interceptors::set_write(FailOnce<5> {"test/wal-"});
    run_abort_restores_state_test(*this);
}

template<class Test>
static auto run_abort_is_reentrant_test(Test &test, int &counter, int &counter_max) -> void
{
    auto xact = test.db.transaction();
    auto s = modify_until_failure(test);
    assert_error_42(s);
    Size fail_count {};
    counter_max = 0;

    for (; ; ) {
        counter = 0;
        counter_max++;
        if (xact.abort().is_ok())
            break;
        s = test.db.status();
        assert_error_42(s);
        fail_count++;
    }
    ASSERT_GT(fail_count, 5);
    ASSERT_TRUE(expose_message(test.db.status()));

    interceptors::reset();
}

TEST_F(FailureTests, AbortIsReentrantForDataReadErrors)
{
    int counter {};
    int counter_max {10};
    interceptors::read = [&counter, &counter_max](const std::string &path, Bytes&, Size) {
        if (path != "test/data")
            return Status::ok();
        return counter++ == counter_max ? Status::system_error("42") : Status::ok();
    };

    run_abort_is_reentrant_test(*this, counter, counter_max);
}

TEST_F(FailureTests, AbortIsReentrantForDataWriteErrors)
{
    int counter {};
    int counter_max {10};
    interceptors::write = [&counter, &counter_max](const std::string &path, BytesView, Size) {
        if (path != "test/data")
            return Status::ok();
        return counter++ == counter_max ? Status::system_error("42") : Status::ok();
    };

    run_abort_is_reentrant_test(*this, counter, counter_max);
}

TEST_F(FailureTests, AbortRestoresStateAfterDataReadError_Atomic)
{
    interceptors::set_read(FailOnce<2> {"test/data"});
    assert_error_42(modify_until_failure(*this));
    ASSERT_TRUE(expose_message(db.status()));
}

TEST_F(FailureTests, AbortRestoresStateAfterDataWriteError_Atomic)
{
    interceptors::set_write(FailOnce<5> {"test/data"});
    assert_error_42(modify_until_failure(*this));
    ASSERT_TRUE(expose_message(db.status()));
}

class RecoveryTests: public TestOnHeap {
public:
    RecoveryTests()
    {
        options.page_size = 0x200;
        options.frame_count = 16;
    }

    ~RecoveryTests() override
    {
        interceptors::reset();
    }

    auto SetUp() -> void override
    {
        ASSERT_TRUE(expose_message(open_database()));

        RecordGenerator::Parameters param;
        param.mean_key_size = 16;
        param.mean_value_size = 100;
        param.is_unique = true;
        param.spread = 0;
        RecordGenerator generator {param};
        Random random {internal::random_seed};

        static constexpr Size GROUP_SIZE {1'000};
        uncommitted = generator.generate(random, GROUP_SIZE * 2);
        committed.insert(cend(committed), cbegin(uncommitted) + GROUP_SIZE, cend(uncommitted));
        uncommitted.resize(GROUP_SIZE);

        // Transaction needs to go out of scope before the database is closed, hence the block.
        {
            static constexpr Size NUM_XACTS {10};
            static constexpr auto XACT_SIZE = GROUP_SIZE / NUM_XACTS;
            static_assert(GROUP_SIZE % XACT_SIZE == 0);

            // Commit 10 transactions.
            auto begin = cbegin(committed);
            while (begin != cend(committed)) {
                auto xact = db.transaction();
                for (auto itr = begin; itr != begin + XACT_SIZE; ++itr) {
                    ASSERT_TRUE(expose_message(db.insert(itr->key, itr->value)));
                }
                ASSERT_TRUE(expose_message(xact.commit()));
                begin += XACT_SIZE;
            }
            // Fail on the next write to the data file.
            interceptors::write = [](const std::string &path, BytesView, Size) {
                return path != "test/data" ? Status::ok() : Status::system_error("42");
            };

            auto xact = db.transaction();
            for (const auto &[key, value]: uncommitted) {
                const auto s = db.insert(key, value);
                if (!s.is_ok()) {
                    ASSERT_EQ(s.what(), "42");
                    break;
                }
            }
            ASSERT_EQ(xact.abort().what(), "42");
        }
        ASSERT_EQ(db.close().what(), "42");

        interceptors::write = [](auto, auto, auto) {
            return Status::ok();
        };
    }

    auto open_database() -> Status
    {
        options.store = store.get();
        return db.open(ROOT, options);
    }

    auto validate() -> void
    {
        db.tree().TEST_validate_nodes();
        db.tree().TEST_validate_links();
        db.tree().TEST_validate_order();

        for (const auto &[key, value]: committed) {
            ASSERT_TRUE(tools::contains(db, key, value)) << "database should contain " << key;
        }
        for (const auto &[key, value]: uncommitted) {
            ASSERT_FALSE(db.find_exact(key).is_valid()) << "database should not contain " << key;
        }
    }

    std::vector<Record> committed;
    std::vector<Record> uncommitted;
    Options options;
    Core db;
};

TEST_F(RecoveryTests, BasicRecoveryWorks)
{
    open_database();
    validate();
}

template<class Test>
static auto run_recovery_is_reentrant_test(Test &test, int step, int &counter, int &target) -> void
{
    Size num_tries {};
    for (; ; num_tries++) {
        auto s = test.open_database();
        if (s.is_ok()) {
            ASSERT_GT(target, step);
            target = 0;
            counter = 1;
            test.validate();
            break;
        } else {
            ASSERT_TRUE(s.is_system_error());
            ASSERT_EQ(s.what(), "42");
            counter = 0;
            target += step;
        }
    }
    ASSERT_GE(num_tries, 3);
}

TEST_F(RecoveryTests, RecoveryIsReentrantForDataReadFaults)
{
    static constexpr auto START {100};
    int counter {};
    int target {START};

    interceptors::read = [&counter, &target](const std::string &path, Bytes&, Size) {
        if (path != "test/data")
            return Status::ok();
        return counter++ == target ? Status::system_error("42") : Status::ok();
    };

    run_recovery_is_reentrant_test(*this, START, counter, target);
}

TEST_F(RecoveryTests, RecoveryIsReentrantForDataWriteFaults)
{
    static constexpr auto START {100};
    int counter {};
    int target {START};

    interceptors::write = [&counter, &target](const std::string &path, BytesView, Size) {
        if (path != "test/data")
            return Status::ok();
        return counter++ == target ? Status::system_error("42") : Status::ok();
    };

    run_recovery_is_reentrant_test(*this, START, counter, target);
}

TEST_F(RecoveryTests, RecoveryIsReentrantForWalReadFaults)
{
    static constexpr auto START {100};
    int counter {};
    int target {START};

    interceptors::read = [&counter, &target](const std::string &path, Bytes&, Size) {
        if (path == "test/data")
            return Status::ok();
        return counter++ == target ? Status::system_error("42") : Status::ok();
    };

    run_recovery_is_reentrant_test(*this, START, counter, target);
}

} // namespace calico