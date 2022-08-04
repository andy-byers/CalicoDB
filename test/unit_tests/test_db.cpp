
#include <array>
#include <filesystem>
#include <vector>
#include <unordered_set>

#include <gtest/gtest.h>

#include "db/database_impl.h"
#include "storage/system.h"

#include "tree/tree.h"
#include "fakes.h"

#include "tools.h"
#include "utils/logging.h"

namespace {

using namespace cco;
namespace fs = std::filesystem;
constexpr auto BASE = "/tmp/__calico_database_tests";

class TestDatabase {
public:
    TestDatabase()
    {
        std::unique_ptr<IDirectory> home;
        Database::Impl::Parameters param;
        param.options.page_size = 0x200;
        param.options.frame_count = 16;

        home = std::make_unique<FakeDirectory>("");
        impl = Database::Impl::open(param, std::move(home)).value();
        fake = dynamic_cast<FakeDirectory*>(&impl->home());
        data_controls = fake->get_faults("data");

        RecordGenerator::Parameters generator_param;
        generator_param.mean_key_size = 20;
        generator_param.mean_value_size = 50;
        generator_param.spread = 15;
        auto generator = RecordGenerator {generator_param};

        records = generator.generate(random, 1'500);
        for (const auto &[key, value]: records)
            tools::insert(*impl, key, value);
        std::sort(begin(records), end(records));
    }

    ~TestDatabase()
    {
        data_controls.set_read_fault_rate(0);
        fake->get_faults("wal-latest").set_read_fault_rate(0);
        data_controls.set_read_fault_counter(-1);
        fake->get_faults("wal-latest").set_read_fault_counter(-1);

        data_controls.set_write_fault_rate(0);
        fake->get_faults("wal-latest").set_write_fault_rate(0);
        data_controls.set_write_fault_counter(-1);
        fake->get_faults("wal-latest").set_write_fault_counter(-1);
    }

    auto remove_one(const std::string &key) -> Result<void>
    {
        CCO_EXPECT_GT(impl->info().record_count(), 0);
        CCO_TRY_CREATE(was_erased, impl->erase(impl->find(stob(key))));
        if (!was_erased) {
            CCO_TRY_STORE(was_erased, impl->erase(impl->find_minimum()));
            EXPECT_TRUE(was_erased);
        }
        return {};
    }

    Random random {0};
    FakeDirectory *fake {};
    FaultControls data_controls {};
    std::vector<Record> records;
    std::unique_ptr<Database::Impl> impl;
};

class DatabaseReadFaultTests: public testing::Test {
public:
    DatabaseReadFaultTests() = default;
    ~DatabaseReadFaultTests() override = default;

    TestDatabase db;
};

TEST(DatabaseOpenTest, ReportsInvalidPageSizes)
{
    Options options;
    options.path = "/tmp/calico_test_db__";
    std::error_code ignore;

    fs::remove_all(options.path, ignore);
    options.page_size = MINIMUM_PAGE_SIZE / 2;
    Database db {options};
    ASSERT_TRUE(db.open().is_invalid_argument());
    ASSERT_FALSE(db.is_open());

    fs::remove_all(options.path, ignore); // TODO: We shouldn't need to remove the database directory each time.
    options.page_size = MAXIMUM_PAGE_SIZE * 2;  //       If we fail during construction of a new database, we should
    db = Database {options};                    //       clean up after ourselves properly.
    ASSERT_TRUE(db.open().is_invalid_argument());
    ASSERT_FALSE(db.is_open());

    fs::remove_all(options.path, ignore);
    options.page_size = DEFAULT_PAGE_SIZE - 1;
    db = Database {options};
    ASSERT_TRUE(db.open().is_invalid_argument());
    ASSERT_FALSE(db.is_open());
}

TEST(DatabaseOpenTest, ReportsInvalidFrameCounts)
{
    Options options;
    options.path = "/tmp/calico_test_db__";
    std::error_code ignore;

    fs::remove_all(options.path, ignore);
    options.frame_count = MINIMUM_FRAME_COUNT - 1;
    Database db {options};
    ASSERT_TRUE(db.open().is_invalid_argument());
    ASSERT_FALSE(db.is_open());

    fs::remove_all(options.path, ignore);
    options.frame_count = MAXIMUM_FRAME_COUNT + 1;
    db = Database {options};
    ASSERT_TRUE(db.open().is_invalid_argument());
    ASSERT_FALSE(db.is_open());
}

TEST_F(DatabaseReadFaultTests, OperationsAfterAbort)
{
    ASSERT_TRUE(db.impl->commit());

    const auto info = db.impl->info();
    const auto half = info.record_count() / 2;
    ASSERT_GT(half, 0);

    while (info.record_count() > half)
        ASSERT_TRUE(db.impl->erase(db.impl->find_minimum()));

    auto r = db.impl->abort();
    ASSERT_TRUE(r) << "Error: " << r.error().what();

    for (const auto &[key, value]: db.records) {
        auto c = tools::find(*db.impl, key);
        ASSERT_EQ(btos(c.key()), key);
        ASSERT_EQ(c.value(), value);
    }
}

TEST_F(DatabaseReadFaultTests, SystemErrorIsStoredInCursor)
{
    auto cursor = db.impl->find_minimum();
    ASSERT_TRUE(cursor.is_valid());
    db.data_controls.set_read_fault_rate(100);
    while (cursor.increment()) {}
    ASSERT_FALSE(cursor.is_valid());
    ASSERT_TRUE(cursor.status().is_system_error());
}

TEST_F(DatabaseReadFaultTests, StateIsUnaffectedByReadFaults)
{
    static constexpr auto STEP = 10;

    // We need to commit before we encounter a system error. The current implementation will lock up
    // if one is encountered while in the middle of a transaction.
    ASSERT_TRUE(db.impl->commit());

    unsigned r {}, num_faults {};
    for (; r <= 100; r += STEP) {
        db.data_controls.set_read_fault_rate(100 - r);
        auto cursor = db.impl->find_minimum();
        while (cursor.increment()) {}
        ASSERT_FALSE(cursor.is_valid());
        num_faults += !cursor.status().is_ok();
    }
    ASSERT_GT(num_faults, 0);

    db.data_controls.set_read_fault_rate(0);
    for (const auto &[key, value]: db.records) {
        auto cursor = tools::find(*db.impl, key);
        ASSERT_TRUE(cursor.is_valid());
        ASSERT_EQ(cursor.value(), value);
    }
}

class DatabaseWriteFaultTests: public testing::Test {
public:
    DatabaseWriteFaultTests()
    {
        EXPECT_TRUE(db.impl->commit());

        // Mess up the database.
        auto generator = RecordGenerator {{}};
        for (const auto &[key, value]: generator.generate(db.random, 2'500)) {
            if (const auto r = db.random.next_int(8); r == 0) {
                EXPECT_TRUE(db.impl->erase(db.impl->find_minimum()));
            } else if (r == 1) {
                EXPECT_TRUE(db.impl->erase(db.impl->find_maximum()));
            }
            EXPECT_TRUE(tools::insert(*db.impl, key, value));
        }
    }
    ~DatabaseWriteFaultTests() override = default;

    std::vector<Record> uncommitted;
    TestDatabase db;
};

TEST_F(DatabaseWriteFaultTests, InvalidArgumentErrorsDoNotCauseLockup)
{
    const auto empty_key_result = db.impl->insert(stob(""), stob("value"));
    ASSERT_FALSE(empty_key_result.has_value());
    ASSERT_TRUE(empty_key_result.error().is_invalid_argument());
    ASSERT_TRUE(db.impl->insert(stob("*"), stob("value")));

    const std::string long_key(db.impl->info().maximum_key_size() + 1, 'x');
    const auto long_key_result = db.impl->insert(stob(long_key), stob("value"));
    ASSERT_FALSE(long_key_result.has_value());
    ASSERT_TRUE(long_key_result.error().is_invalid_argument());
    ASSERT_TRUE(db.impl->insert(stob(long_key).truncate(long_key.size() - 1), stob("value")));
}

template<class RateSetter>
auto abort_until_successful(TestDatabase &db, RateSetter &&setter)
{
    for (unsigned rate {100}; rate >= 50; rate -= 10) {
        setter(rate);

        if (const auto r = db.impl->abort()) {
            ASSERT_LT(rate, 100) << "Abort succeeded with an error rate of 100%";
            setter(0);
            return;
        } else {
            ASSERT_TRUE(r.error().is_system_error()) << "Unexpected error: " << r.error().what();
        }
    }
    setter(0);
    ASSERT_TRUE(db.impl->abort());
}

auto validate_after_abort(TestDatabase &db)
{
    // db.records contains the set of records in the database after the first commit. The constructor for the "write fault tests"
    // adds some records and deletes others, so if abort() didn't do its job, the database will contain different records. Removing
    // all the records here makes sure the tree connections are still valid.
    for (const auto &[key, value]: db.records) {
        auto cursor = tools::find(*db.impl, key);
        ASSERT_TRUE(cursor.is_valid());
        ASSERT_EQ(cursor.value(), value);
        ASSERT_TRUE(db.impl->erase(cursor));
    }
    ASSERT_EQ(db.impl->info().record_count(), 0);
}

TEST_F(DatabaseWriteFaultTests, AbortIsReentrantAfterDataWriteFaults)
{
    abort_until_successful(db, [this](unsigned rate) {
        db.data_controls.set_write_fault_rate(rate);
    });
    validate_after_abort(db);
}

TEST_F(DatabaseWriteFaultTests, AbortIsReentrantAfterWALReadFaults)
{
    abort_until_successful(db, [this](unsigned rate) {
        db.fake->get_faults("wal-latest").set_read_fault_rate(rate);
    });
    validate_after_abort(db);
}

TEST_F(DatabaseWriteFaultTests, AbortFixesLockup)
{
    db.data_controls.set_write_fault_rate(100);
    for (Index i {}; ; ++i) {
        const auto s = std::to_string(i);
        auto result = db.impl->insert(stob(s), stob(s));
        if (!result.has_value()) {
            // The following operations should fail until an abort() call is successful.
            ASSERT_TRUE(db.impl->insert(stob(s), stob(s)).error().is_system_error());
            ASSERT_TRUE(db.impl->erase(stob(s)).error().is_system_error());
            ASSERT_TRUE(db.impl->find(stob(s)).status().is_system_error());
            ASSERT_TRUE(db.impl->find_minimum().status().is_system_error());
            ASSERT_TRUE(db.impl->find_maximum().status().is_system_error());
            ASSERT_TRUE(db.impl->commit().error().is_system_error());
            break;
        }
    }
    // Might as well let it fail a few times. abort() should be reentrant anyway.
    while (!db.impl->abort().has_value()) {
        const auto rate = db.data_controls.write_fault_rate();
        db.data_controls.set_write_fault_rate(2 * rate / 3);
    }

    validate_after_abort(db);
}

class DatabaseTests: public testing::Test {
public:
    DatabaseTests()
    {
        options.path = BASE;
        options.page_size = 0x200;
        options.frame_count = 16;

        RecordGenerator::Parameters param;
        param.mean_key_size = 20;
        param.mean_value_size = 20;
        param.spread = 15;
        generator = RecordGenerator {param};

        // Make sure the database does not exist already.
        std::error_code ignore;
        fs::remove_all(BASE, ignore);
    }

    Random random {0};
    Options options;
    RecordGenerator generator;
};

TEST_F(DatabaseTests, NewDatabase)
{
    Database db {options};
    ASSERT_TRUE(db.open().is_ok());
    const auto info = db.info();
    ASSERT_EQ(info.record_count(), 0);
    ASSERT_EQ(info.page_count(), 1);
    ASSERT_NE(info.cache_hit_ratio(), 0.0);
    ASSERT_TRUE(info.uses_xact());
    ASSERT_FALSE(info.is_temp());
    ASSERT_TRUE(db.close().is_ok());
}

TEST_F(DatabaseTests, ReopenDatabase)
{
    Database db {options};
    ASSERT_TRUE(db.open().is_ok());
    ASSERT_TRUE(db.close().is_ok());

    ASSERT_TRUE(db.open().is_ok());
    ASSERT_EQ(db.info().record_count(), 0);
    ASSERT_TRUE(db.close().is_ok());
}

TEST_F(DatabaseTests, Inserts)
{
    static constexpr Size NUM_ITERATIONS {5};
    static constexpr Size GROUP_SIZE {500};

    Database db {options};
    ASSERT_TRUE(db.open().is_ok());

    for (Index iteration {}; iteration < NUM_ITERATIONS; ++iteration) {
        const auto records = generator.generate(random, GROUP_SIZE);
        auto itr = std::cbegin(records);

        for (Index i {}; i < GROUP_SIZE; ++i) {
            ASSERT_TRUE(db.insert(*itr).is_ok());
            itr++;
        }
        ASSERT_TRUE(db.commit().is_ok());
    }
    ASSERT_TRUE(db.close().is_ok());
}

TEST_F(DatabaseTests, DataPersists)
{
    static constexpr Size NUM_ITERATIONS {5};
    static constexpr Size GROUP_SIZE {500};

    const auto records = generator.generate(random, GROUP_SIZE * NUM_ITERATIONS);
    auto itr = std::cbegin(records);

    for (Index iteration {}; iteration < NUM_ITERATIONS; ++iteration) {
        Database db {options};
        ASSERT_TRUE(db.open().is_ok());

        for (Index i {}; i < GROUP_SIZE; ++i) {
            ASSERT_TRUE(db.insert(*itr).is_ok());
            itr++;
        }
        ASSERT_TRUE(db.close().is_ok());
    }

    Database db {options};
    ASSERT_TRUE(db.open().is_ok());
    CCO_EXPECT_EQ(db.info().record_count(), records.size());
    for (const auto &[key, value]: records) {
        const auto c = tools::find_exact(db, key);
        ASSERT_TRUE(c.is_valid());
        ASSERT_EQ(btos(c.key()), key);
        ASSERT_EQ(c.value(), value);
    }
    ASSERT_TRUE(db.close().is_ok());
}

TEST_F(DatabaseTests, CannotCommitEmptyTransaction)
{
    Options options;
    options.path = "/tmp/__database_commit_test";
    Database db {options};
    CCO_EXPECT_OK(db.open());
    CCO_EXPECT_OK(db.insert("a", "1"));
    CCO_EXPECT_OK(db.insert("b", "2"));
    CCO_EXPECT_OK(db.insert("c", "3"));
    CCO_EXPECT_OK(db.commit());

    const auto s = db.commit();
    ASSERT_TRUE(s.is_logic_error()) << "Error: " << (s.is_ok() ? "commit() should have failed" : s.what());
    const auto info = db.info();
    ASSERT_EQ(info.record_count(), 3);
    ASSERT_EQ(info.page_count(), 1);
}

TEST_F(DatabaseTests, DatabaseRecovers)
{
    static constexpr Size GROUP_SIZE {500};
    Options options;
    options.path = BASE;
    options.page_size = 0x400;
    options.frame_count = 16;

    RecordGenerator::Parameters param;
    param.mean_key_size = 40;
    param.mean_value_size = 20;
    param.spread = 20;
    param.is_unique = true;
    RecordGenerator generator {param};
    Random random {0};

    // Make sure the database does not exist already.
    std::error_code ignore;
    const auto alternate = std::string {BASE} + "_";
    fs::remove_all(BASE, ignore);
    fs::remove_all(alternate, ignore);

    Database db {options};
    ASSERT_TRUE(db.open().is_ok());

    const auto all_records = generator.generate(random, GROUP_SIZE * 2);
    for (auto itr = begin(all_records); itr != begin(all_records) + GROUP_SIZE; ++itr) {
        const auto s = db.insert(*itr);
        ASSERT_TRUE(s.is_ok()) << "Error: " << s.what();
    }

    ASSERT_TRUE(db.commit().is_ok());

    for (auto itr = begin(all_records) + GROUP_SIZE; itr != end(all_records); ++itr)
        ASSERT_TRUE(db.insert(*itr).is_ok());

    fs::copy(BASE, alternate);
    ASSERT_TRUE(db.close().is_ok());

    options.path = alternate;
    db = Database {options};
    ASSERT_TRUE(db.open().is_ok());

    for (auto itr = begin(all_records); itr != begin(all_records) + GROUP_SIZE; ++itr) {
        const auto c = db.find_exact(itr->key);
        ASSERT_TRUE(c.is_valid());
        ASSERT_EQ(c.value(), itr->value);
    }
}


//class DatabaseSanityCheck: public testing::TestWithParam<Options> {
//public:
//    auto run()
//    {
//        static constexpr Size GROUP_SIZE {50};
//        static constexpr Size NUM_ITERATIONS {30};
//        RecordGenerator::Parameters param;
//        param.mean_key_size = 20;
//        param.mean_value_size = 20;
//        param.spread = 15;
//        param.is_unique = true;
//        RecordGenerator generator {param};
//        Random random {0};
//
//        // Make sure the database does not exist already.
//        std::error_code ignore;
//        fs::remove_all(BASE, ignore);
//
//        Database db {GetParam()};
//        ASSERT_TRUE(db.open().is_ok());
//
//        const auto records = generator.generate(random, GROUP_SIZE * NUM_ITERATIONS);
//        std::vector<Record> committed;
//
//        auto parity = 0;
//        for (auto itr = cbegin(records); itr + GROUP_SIZE != cend(records); itr += GROUP_SIZE) {
//            for (auto record = itr; record != itr + GROUP_SIZE; ++record) {
//                ASSERT_TRUE(db.insert(*record).is_ok());
//            }
//            if (++parity & 1) {
//                committed.insert(end(committed), itr, itr + GROUP_SIZE);
//                ASSERT_TRUE(db.commit().is_ok());
//            } else {
//                ASSERT_TRUE(db.abort().is_ok());
//            }
//            itr += GROUP_SIZE;
//        }
//
//        for (const auto &record: committed) {
//            const auto c = db.find_exact(record.key);
//            ASSERT_TRUE(c.is_valid());
//            ASSERT_EQ(c.value(), record.value);
//        }
//    }
//};
//
//TEST_P(DatabaseSanityCheck, Transactions)
//{
//    run();
//}
//
//
//INSTANTIATE_TEST_SUITE_P(
//    TransactionTestCase,
//    DatabaseSanityCheck,
//    ::testing::Values(
//        Options {"/tmp/__calico_transactions", 0x100, 16, 0644}
////        Options {"", 0x100, 16, 0644}
//        ));

class MockDatabase {
public:
    MockDatabase()
    {
        using testing::_;
        using testing::AtLeast;
        using testing::StartsWith;

        Database::Impl::Parameters param;
        param.options.page_size = 0x200;
        param.options.frame_count = 16;

        auto temp = std::make_unique<MockDirectory>("MockDatabase");
        EXPECT_CALL(*temp, open_file("data", _, _)).Times(1);
        EXPECT_CALL(*temp, open_file(StartsWith("wal"), _, _)).Times(AtLeast(2));
        EXPECT_CALL(*temp, remove_file(StartsWith("wal"))).Times(AtLeast(0));
        EXPECT_CALL(*temp, exists(_)).Times(AtLeast(1));
        EXPECT_CALL(*temp, children).Times(1);
        EXPECT_CALL(*temp, close).Times(1);
        impl = Database::Impl::open(param, std::move(temp)).value();
        mock = dynamic_cast<MockDirectory*>(&impl->home());
        data_mock = mock->get_mock_data_file();

        RecordGenerator::Parameters generator_param;
        generator_param.mean_key_size = 20;
        generator_param.mean_value_size = 50;
        generator_param.spread = 15;
        auto generator = RecordGenerator {generator_param};

        records = generator.generate(random, 1'500);
        for (const auto &[key, value]: records)
            tools::insert(*impl, key, value);
        std::sort(begin(records), end(records));
    }

    ~MockDatabase() = default;

    auto remove_one(const std::string &key) -> Result<void>
    {
        CCO_EXPECT_GT(impl->info().record_count(), 0);
        CCO_TRY_CREATE(was_erased, impl->erase(impl->find(stob(key))));
        if (!was_erased) {
            CCO_TRY_STORE(was_erased, impl->erase(impl->find_minimum()));
            EXPECT_TRUE(was_erased);
        }
        return {};
    }

    Random random {0};
    MockDirectory *mock {};
    MockFile *data_mock {};
    std::vector<Record> records;
    std::unique_ptr<Database::Impl> impl;
};

TEST(MockDatabaseTests, CommitSmallTransactions)
{
    MockDatabase db;
    const auto info = db.impl->info();
    ASSERT_TRUE(db.impl->commit());
    const auto record_count = info.record_count();

    for (Index i {}; i < 10; ++i) {
        ASSERT_TRUE(db.impl->erase(db.impl->find_minimum()));
        ASSERT_TRUE(db.impl->erase(db.impl->find_maximum()));
        ASSERT_TRUE(db.impl->commit());
        ASSERT_EQ(info.record_count(), record_count - 2*(i+1));
    }
}

TEST(MockDatabaseTests, AbortSmallTransactions)
{
    MockDatabase db;
    const auto info = db.impl->info();
    ASSERT_TRUE(db.impl->commit());
    const auto record_count = info.record_count();
    auto left = cbegin(db.records);
    auto right = crbegin(db.records);
    for (Index i {}; i < 10; ++i, ++left, ++right) {
        ASSERT_TRUE(db.impl->erase(db.impl->find(stob(left->key))));
        ASSERT_TRUE(db.impl->erase(db.impl->find(stob(right->key))));
        ASSERT_TRUE(db.impl->abort());
        ASSERT_EQ(info.record_count(), record_count);
    }
    for (const auto &[key, value]: db.records) {
        auto c = db.impl->find_exact(stob(key));
        ASSERT_TRUE(c.is_valid());
        ASSERT_EQ(c.value(), value);
    }
}

TEST(MockDatabaseTests, RecoversFromFailedCommit)
{
    using testing::_;
    using testing::Return;

    MockDatabase db;
    auto wal_mock = db.mock->get_mock_wal_writer_file("latest");
    ON_CALL(*wal_mock, write(_))
        .WillByDefault(Return(Err {Status::system_error("123")}));

    auto r = db.impl->commit();
    ASSERT_FALSE(r.has_value()) << "commit() should have failed";
    ASSERT_TRUE(r.error().is_system_error()) << "Unexpected error: " << r.error().what();
    ASSERT_EQ(r.error().what(), "123");
    ASSERT_EQ(db.impl->status().what(), "123") << "System error should be stored in database status";

    wal_mock->delegate_to_fake();
    r = db.impl->abort();
    ASSERT_TRUE(r.has_value());
    ASSERT_TRUE(db.impl->status().is_ok());
}

//TEST(MockDatabaseTests, SanityCheck)
//{
//    static constexpr Size NUM_ITERATIONS {10};
//    static constexpr Size GROUP_SIZE {500};
//    using testing::_;
//    using testing::Return;
//
//    MockDatabase db;
//    auto remaining = db.records;
//    for (auto itr = cbegin(db.records); itr != cend(db.records); ++itr) {
//        auto n = db.random.next_int(50);
//        while (itr != cend(db.records) && n-- > 0) {
//
//        }
//    }
//
//    auto wal_mock = db.mock->get_mock_wal_writer_file("latest");
//    ON_CALL(*wal_mock, write(_))
//        .WillByDefault(Return(Err {Status::system_error("123")}));
//
//    auto r = db.impl->commit();
//    ASSERT_FALSE(r.has_value()) << "commit() should have failed";
//    ASSERT_TRUE(r.error().is_system_error()) << "Unexpected error: " << r.error().what();
//    ASSERT_EQ(r.error().what(), "123");
//    ASSERT_EQ(db.impl->status().what(), "123") << "System error should be stored in database status";
//
//    wal_mock->delegate_to_fake();
//    r = db.impl->abort();
//    ASSERT_TRUE(r.has_value());
//    ASSERT_TRUE(db.impl->status().is_ok());
//}

template<class Mock>
auto run_close_error_test(MockDatabase &db, Mock &mock)
{
    using testing::Return;

    ON_CALL(mock, close)
        .WillByDefault(Return(Err {Status::system_error("123")}));

    const auto s = db.impl->close();
    ASSERT_FALSE(s.has_value());
    ASSERT_TRUE(s.error().is_system_error());
    ASSERT_EQ(s.error().what(), "123");
    ASSERT_TRUE(db.impl->status().is_system_error());
    ASSERT_EQ(db.impl->status().what(), "123");
}

TEST(MockDatabaseTests, PropagatesErrorFromWALClose)
{
    MockDatabase db;
    run_close_error_test(db, *db.mock->get_mock_wal_writer_file("latest"));
}

TEST(RealDatabaseTests, DestroyDatabase)
{
    Options options;
    options.path = "/tmp/calico_test_db__";
    std::error_code ignore;
    fs::remove_all(options.path, ignore);
    Database db {options};
    ASSERT_TRUE(db.open().is_ok());
    ASSERT_TRUE(Database::destroy(std::move(db)).is_ok());
    ASSERT_FALSE(fs::exists(options.path, ignore));
    ASSERT_FALSE(ignore);
}

TEST(RealDatabaseTests, CanDestroyClosedDatabase)
{
    Database db {{}};
    ASSERT_TRUE(db.open().is_ok());
    ASSERT_TRUE(db.close().is_ok());
    ASSERT_TRUE(Database::destroy(std::move(db)).is_ok());
}

TEST(RealDatabaseTests, DatabaseObjectTypes)
{
    Options options;
    Database val {options};
    ASSERT_TRUE(val.open().is_ok());
    ASSERT_TRUE(val.close().is_ok());
    ASSERT_TRUE(Database::destroy(std::move(val)).is_ok());

    auto ptr = std::make_unique<Database>(options);
    ASSERT_TRUE(ptr->open().is_ok());
    ASSERT_TRUE(ptr->close().is_ok());
    ASSERT_TRUE(Database::destroy(std::move(*ptr)).is_ok());
}

class RecoveryTests: public testing::TestWithParam<Size> {
public:
    static constexpr auto SOURCE = "/tmp/__calico_recovery_tests";
    static constexpr auto TARGET = "/tmp/__calico_recovery_tests_alt";

    RecoveryTests()
    {
        std::error_code ignore;
        fs::remove_all(SOURCE, ignore);
        fs::remove_all(TARGET, ignore);
        CCO_EXPECT_OK(db.open());
    }

    ~RecoveryTests() override
    {
        EXPECT_EQ(options.path, TARGET) << "fail_and_reopen() was never called";
        EXPECT_TRUE(db.is_open()) << "Database should be open";
        std::error_code ignore;
        fs::remove_all(SOURCE, ignore);
        CCO_EXPECT_OK(Database::destroy(std::move(db)));
    }

    auto fail_and_recover() -> Status
    {
        EXPECT_EQ(options.path, SOURCE) << "fail_and_reopen() was called more than once";
        fs::copy(SOURCE, TARGET);
        CCO_EXPECT_OK(db.close());
        options.path = TARGET;
        db = Database {options};
        return db.open();
    }

    Options options {
        SOURCE,
        0x400,
        16,
        0666,
        true,
    };

    Database db {options};
};

TEST_F(RecoveryTests, RollsBackUncommittedTransaction)
{
    CCO_EXPECT_OK(db.insert("a", "1"));
    CCO_EXPECT_OK(db.insert("b", "2"));
    CCO_EXPECT_OK(db.insert("c", "3"));
    CCO_EXPECT_OK(fail_and_recover());
    const auto info = db.info();
    ASSERT_EQ(info.record_count(), 0);
    ASSERT_EQ(info.page_count(), 1);
}

TEST_F(RecoveryTests, PreservesCommittedTransaction)
{
    CCO_EXPECT_OK(db.insert("a", "1"));
    CCO_EXPECT_OK(db.insert("b", "2"));
    CCO_EXPECT_OK(db.insert("c", "3"));
    CCO_EXPECT_OK(db.commit());
    CCO_EXPECT_OK(fail_and_recover());
    const auto info = db.info();
    ASSERT_EQ(info.record_count(), 3);
    ASSERT_EQ(info.page_count(), 1);
}

} // <anonymous>
