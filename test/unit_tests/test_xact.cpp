
#include <fstream>
#include <gtest/gtest.h>

#include "calico/bytes.h"
#include "calico/options.h"
#include "calico/store.h"
#include "core/core.h"
#include "pager/basic_pager.h"
#include "tree/tree.h"
#include "unit_tests.h"
#include "fakes.h"
#include "tools.h"

namespace {

using namespace calico;
namespace fs = std::filesystem;

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
        ASSERT_TRUE(expose_message(db.close()));
    }

    RecordGenerator generator {{16, 100, 10, false, true}};
    Random random {123};
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

static auto insert_1000_records(XactTests &test)
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
        if (test.random.next_int(5) == 0) {
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
        if (test.random.next_int(4UL) == 0) {
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

class FailureTests: public TestWithMock {
public:
    FailureTests() = default;

    ~FailureTests() override = default;

    auto SetUp() -> void override
    {
        Options options;
        options.page_size = 0x200;
        options.frame_count = 16;
        options.store = store.get();
        ASSERT_TRUE(expose_message(db.open(ROOT, options)));
        editor_mock = mock_store().get_mock_random_editor(PREFIX + std::string {DATA_FILENAME});
    }

    [[nodiscard]]
    auto data_mock() -> MockRandomEditor*
    {
        return editor_mock;
    }

    [[nodiscard]]
    auto wal_writer_mock(SegmentId id) -> MockAppendWriter*
    {
        return mock_store().get_mock_append_writer(PREFIX + id.to_name()); // TODO: Need some way to get mocks for the latest WAL segment...
                                                                        //       This won't work right now!
    }

    MockRandomEditor *editor_mock {};
    Random random {42};
    Database db;
};

auto assert_is_failure_status(const Status &s)
{
    ASSERT_TRUE(s.is_system_error() and s.what() == "42") << s.what();
}

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
            // insert()/erase() exercises data file reading and writing, and WAL file writing.
            if (test.random.next_int(4) == 0 && info.record_count()) {
                s = test.db.erase(test.db.first());
            } else {
                s = test.db.insert(key, value);
            }
            if (!s.is_ok()) return s;
        }
    }
    return Status::ok();
}

TEST_F(FailureTests, DataReadErrorIsPropagatedDuringModify)
{
    int counter {};
    ON_CALL(*editor_mock, read)
        .WillByDefault(testing::Invoke([&counter, this](Bytes &out, Size offset) {
            return counter++ < 5 ? editor_mock->real().read(out, offset) : Status::system_error("42");
        }));

    // Modify the database until a write() call fails.
    auto xact = db.transaction();
    auto s = modify_until_failure(*this);
    assert_is_failure_status(s);

    // The database status should reflect the error returned by write().
    s = db.status();
    assert_is_failure_status(s);
}

TEST_F(FailureTests, DataWriteErrorIsPropagatedDuringModify)
{
    ON_CALL(*editor_mock, write)
        .WillByDefault(testing::Return(Status::system_error("42")));

    // Modify the database until a write() call fails.
    auto xact = db.transaction();
    auto s = modify_until_failure(*this);
    assert_is_failure_status(s);

    // The database status should reflect the error returned by write().
    s = db.status();
    assert_is_failure_status(s);
}

// TODO: WAL file mocks don't work properly. We don't even create WAL readers until we roll the log and the writer is created in another thread
//       and we don't know when it is finished...
//TEST_F(FailureTests, WalReadErrorIsPropagatedDuringModify)
//{
//    FAIL_ON_NTH_READ(*wal_reader_mock(SegmentId {1}), 5);
//
//    // We need to get the WAL reader to open on the first segment
//    {
//
//    }
//
//    auto xact = db.transaction();
//    auto s = modify_until_failure(*this, xact);
//    assert_is_failure_status(s);
//
//    // The database status should reflect the error returned by write().
//    s = db.status();
//    assert_is_failure_status(s);
//}
//
//TEST_F(FailureTests, WalWriteErrorIsPropagatedDuringModify)
//{
//    // Modify the database until a write() call fails.
//    auto xact = db.transaction();
//
//    FAIL_ON_NTH_APPEND(*wal_writer_mock(SegmentId {1}), 5);
//
//    auto s = modify_until_failure(*this, xact);
//    assert_is_failure_status(s);
//
//    // The database status should reflect the error returned by write().
//    s = db.status();
//    assert_is_failure_status(s);
//}

TEST_F(FailureTests, DataReadErrorIsNotPropagatedDuringQuery)
{
    add_sequential_records(db, 500);
    int counter {};
    ON_CALL(*editor_mock, read)
        .WillByDefault(testing::Invoke([&counter, this](Bytes &out, Size offset) {
            return counter++ < 5 ? editor_mock->real().read(out, offset) : Status::system_error("42");
        }));

    // Iterate until a read() call fails.
    auto c = db.first();
    for (; c.is_valid(); ++c) {}

    // The error in the cursor should reflect the read() error.
    auto s = c.status();
    assert_is_failure_status(s);

    // The database status should still be OK. Errors during reads cannot corrupt or even modify the database state.
    s = db.status();
    ASSERT_TRUE(s.is_ok()) << s.what();
}

// Error encountered while flushing a dirty page to make room for a page read during a query. In this case, we don't have a transaction
// we can try to abort, so we must exit the program. Next time the database is opened, it will roll forward and apply any missing updates.
TEST_F(FailureTests, DataWriteFailureDuringQuery)
{
    add_sequential_records(db, 500);

    // Further writes to the data file will fail.
    ON_CALL(*editor_mock, write)
        .WillByDefault(testing::Return(Status::system_error("42")));

    auto c = db.first();
    for (; c.is_valid(); ++c) {}

    auto s = c.status();
    assert_is_failure_status(s);

    s = db.status();
    assert_is_failure_status(s);
}

TEST_F(FailureTests, DatabaseNeverWritesAfterPagesAreFlushedDuringQuery)
{
    add_sequential_records(db, 500);

    // This will cause all dirty pages to eventually be evicted to make room.
    auto c = db.first();
    for (; c.is_valid(); ++c) {}

    // Further writes to the data file will fail.
    ON_CALL(*editor_mock, write)
        .WillByDefault(testing::Return(Status::system_error("42")));

    // We should be able to iterate through all pages without any writes occurring.
    c = db.first();
    for (; c.is_valid(); ++c) {}

    auto s = c.status();
    ASSERT_TRUE(s.is_not_found()) << s.what();

    s = db.status();
    ASSERT_TRUE(s.is_ok()) << s.what();
}

TEST_F(FailureTests, AbortRestoresStateAfterDataReadError)
{
    int counter {};
    ON_CALL(*editor_mock, read)
        .WillByDefault(testing::Invoke([&counter, this](Bytes &out, Size offset) {
            return counter++ == 2 ? Status::system_error("42") : editor_mock->real().read(out, offset);
        }));

    auto xact = db.transaction();
    auto s = modify_until_failure(*this);
    assert_is_failure_status(s);

    s = db.status();
    assert_is_failure_status(s);

    ASSERT_TRUE(expose_message(xact.abort()));
    ASSERT_TRUE(expose_message(db.status()));
}

TEST_F(FailureTests, AbortRestoresStateAfterDataReadError_Atomic)
{
    int counter {};
    ON_CALL(*editor_mock, read)
        .WillByDefault(testing::Invoke([&counter, this](Bytes &out, Size offset) {
            return counter++ == 2 ? Status::system_error("42") : editor_mock->real().read(out, offset);
        }));

    assert_is_failure_status(modify_until_failure(*this));
    ASSERT_TRUE(expose_message(db.status()));
}

TEST_F(FailureTests, AbortRestoresStateAfterDataWriteError)
{
    int counter {};
    ON_CALL(*editor_mock, write)
        .WillByDefault(testing::Invoke([&counter, this](BytesView in, Size offset) {
            return counter++ == 5 ? Status::system_error("42") : editor_mock->real().write(in, offset);
        }));

    auto xact = db.transaction();
    auto s = modify_until_failure(*this);
    assert_is_failure_status(s);

    s = db.status();
    assert_is_failure_status(s);

    ASSERT_TRUE(expose_message(xact.abort()));
    ASSERT_TRUE(expose_message(db.status()));
}

TEST_F(FailureTests, AbortRestoresStateAfterDataWriteError_Atomic)
{
    int counter {};
    ON_CALL(*editor_mock, write)
        .WillByDefault(testing::Invoke([&counter, this](BytesView in, Size offset) {
            return counter++ == 5 ? Status::system_error("42") : editor_mock->real().write(in, offset);
        }));

    assert_is_failure_status(modify_until_failure(*this));
    ASSERT_TRUE(expose_message(db.status()));
}

// TODO: See above TODO.
////TEST_F(FailureTests, AbortRestoresStateAfterWalReadError)
////{
////    FAIL_ON_N(*data_mock(), write, 5);
////
////    auto s = modify_until_failure(*this, cleanup_with_successful_abort);
////    assert_is_failure_status(s);
////
////    ASSERT_TRUE(expose_message(db.status()));
////}
////
////TEST_F(FailureTests, AbortRestoresStateAfterWalWriteError)
////{
////    FAIL_ON_N(*data_mock(), write, 5);
////
////    auto s = modify_until_failure(*this, cleanup_with_successful_abort);
////    assert_is_failure_status(s);
////
////    ASSERT_TRUE(expose_message(db.status()));
////}

#undef FAIL_ON_NTH_READ
#undef FAIL_ON_NTH_WRITE
#undef FAIL_ON_NTH_APPEND

} // <anonymous>