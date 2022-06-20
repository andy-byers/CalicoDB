
#include <array>
#include <filesystem>
#include <vector>
#include <unordered_set>

#include <gtest/gtest.h>

#include "db/database_impl.h"
#include "db/cursor_impl.h"
#include "file/system.h"
#include "pool/interface.h"
#include "tree/tree.h"

#include "fakes.h"
#include "tools.h"

#include "file/file.h"

namespace {

using namespace cub;


class DatabaseReadTests: public testing::Test {
public:
    static constexpr Size PAGE_SIZE {0x100};

    // Keys used in this test.
    static constexpr auto K0 = "1";
    static constexpr auto K1 = "3";
    static constexpr auto K2 = "5";

    // Keys "minus 1".
    static constexpr auto K0_m1 = "0";
    static constexpr auto K1_m1 = "2";
    static constexpr auto K2_m1 = "4";

    // Keys "plus 1".
    static constexpr auto K0_p1 = "2";
    static constexpr auto K1_p1 = "4";
    static constexpr auto K2_p1 = "6";

    DatabaseReadTests()
        : db {Database::temp(PAGE_SIZE)}
    {
        const auto write = [this](const std::string &key) {
            db.write(stob(key), stob(key));
        };

        write(K0);
        write(K1);
        write(K2);
    }

    ~DatabaseReadTests() override = default;

    auto read_and_compare(const std::string &key, Ordering comparison, const std::string &target)
    {
        if (const auto record = db.read(stob(key), comparison))
            return record->key == target;
        return false;
    }

    Database db;
};

TEST_F(DatabaseReadTests, ReadsExact)
{
    ASSERT_TRUE(read_and_compare(K0, Ordering::EQ, K0));
    ASSERT_TRUE(read_and_compare(K1, Ordering::EQ, K1));
    ASSERT_TRUE(read_and_compare(K2, Ordering::EQ, K2));
    ASSERT_TRUE(read_and_compare(K0, Ordering::LE, K0));
    ASSERT_TRUE(read_and_compare(K1, Ordering::LE, K1));
    ASSERT_TRUE(read_and_compare(K2, Ordering::LE, K2));
    ASSERT_TRUE(read_and_compare(K0, Ordering::GE, K0));
    ASSERT_TRUE(read_and_compare(K1, Ordering::GE, K1));
    ASSERT_TRUE(read_and_compare(K2, Ordering::GE, K2));
}

TEST_F(DatabaseReadTests, ReadsLessThan)
{
    ASSERT_TRUE(read_and_compare(K0_p1, Ordering::LT, K0));
    ASSERT_TRUE(read_and_compare(K1_p1, Ordering::LT, K1));
    ASSERT_TRUE(read_and_compare(K2_p1, Ordering::LT, K2));
    ASSERT_TRUE(read_and_compare(K1, Ordering::LT, K0));
    ASSERT_TRUE(read_and_compare(K2, Ordering::LT, K1));
}

TEST_F(DatabaseReadTests, ReadsGreaterThan)
{
    ASSERT_TRUE(read_and_compare(K0_m1, Ordering::GT, K0));
    ASSERT_TRUE(read_and_compare(K1_m1, Ordering::GT, K1));
    ASSERT_TRUE(read_and_compare(K2_m1, Ordering::GT, K2));
    ASSERT_TRUE(read_and_compare(K0, Ordering::GT, K1));
    ASSERT_TRUE(read_and_compare(K1, Ordering::GT, K2));
}

TEST_F(DatabaseReadTests, CannotReadNonexistentRecords)
{
    ASSERT_EQ(db.read(stob(K0_m1), Ordering::EQ), std::nullopt);
    ASSERT_EQ(db.read(stob(K1_m1), Ordering::EQ), std::nullopt);
    ASSERT_EQ(db.read(stob(K2_m1), Ordering::EQ), std::nullopt);
}

TEST_F(DatabaseReadTests, CannotReadLessThanMinimum)
{
    ASSERT_EQ(db.read(stob(K0), Ordering::LT), std::nullopt);
    ASSERT_EQ(db.read(stob(K0_m1), Ordering::LT), std::nullopt);
}

TEST_F(DatabaseReadTests, CannotReadGreaterThanMaximum)
{
    ASSERT_EQ(db.read(stob(K2), Ordering::GT), std::nullopt);
    ASSERT_EQ(db.read(stob(K2_p1), Ordering::GT), std::nullopt);
}

constexpr auto TEST_PATH = "/tmp/cub_test";

template<class Db> auto database_contains_exact(Db &db, const std::vector<Record> &records)
{
    if (db.get_info().record_count() != records.size())
        return false;

    auto cursor = db.get_cursor();
    for (const auto &[key, value]: records) {
        if (!cursor.find(stob(key)))
            return false;
        if (cursor.key() != stob(key))
            return false;
        if (cursor.value() != value)
            return false;
    }
    return true;
}

template<class Db> auto setup_database_with_committed_records(Db &db, Size n)
{
    DatabaseBuilder builder {&db};
    builder.write_unique_records(n, {});
    return builder.collect_records();
}

class DatabaseTests: public testing::Test {
public:
    DatabaseTests()
    {
        std::filesystem::remove(TEST_PATH);
        std::filesystem::remove(get_wal_path(TEST_PATH));
    }

    ~DatabaseTests() override = default;
};

TEST_F(DatabaseTests, DatabaseDoesNotExistAfterItIsDestroyed)
{
    auto db = Database::open(TEST_PATH, {});
    ASSERT_TRUE(system::exists(TEST_PATH));
    Database::destroy(std::move(db));
    ASSERT_FALSE(system::exists(TEST_PATH));
}

TEST_F(DatabaseTests, AbortRestoresState)
{
    auto db = Database::open(TEST_PATH, {});
    db.write(stob("a"), stob("1"));
    db.write(stob("b"), stob("2"));
    db.commit();

    db.write(stob("c"), stob("3"));
    CUB_EXPECT_TRUE(db.erase(stob("a")));
    CUB_EXPECT_TRUE(db.erase(stob("b")));
    db.abort();

    CUB_EXPECT_EQ(db.read(stob("a"), Ordering::EQ)->value, "1");
    CUB_EXPECT_EQ(db.read(stob("b"), Ordering::EQ)->value, "2");
    CUB_EXPECT_EQ(db.read(stob("c"), Ordering::EQ), std::nullopt);

    const auto info = db.get_info();
    CUB_EXPECT_EQ(info.record_count(), 2);
}

TEST_F(DatabaseTests, CannotAbortIfNotUsingTransactions)
{
    Options options;
    options.use_transactions = false;
    auto db = Database::open(TEST_PATH, options);
    ASSERT_THROW(db.abort(), std::logic_error);
}

TEST_F(DatabaseTests, WALIsNotOpenedIfNotUsingTransactions)
{
    Options options;
    options.use_transactions = false;

    // The second time the database is opened, we should use the file header to determine that we are not
    // using transactions for this database.
    for (int i {}; i < 2; ++i) {
        [[maybe_unused]] auto db = Database::open(TEST_PATH, options);
        ASSERT_FALSE(system::exists(get_wal_path(TEST_PATH)));
    }
}

auto run_persistence_test(const Options &options)
{
    static constexpr Size ROUND_SIZE {200};
    static constexpr Size NUM_ROUNDS {10};

    RecordGenerator::Parameters param;
    param.mean_key_size = 16;
    param.mean_value_size = 100;
    RecordGenerator generator {param};
    Random random {0};
    const auto records = generator.generate(random, ROUND_SIZE * NUM_ROUNDS);
    auto itr = std::cbegin(records);

    for (Index round {}; round < NUM_ROUNDS; ++round) {
        auto db = Database::open(TEST_PATH, options);
        for (Index i {}; i < ROUND_SIZE; ++i) {
            db.write(stob(itr->key), stob(itr->value));
            itr++;
        }
    }

    itr = std::cbegin(records);

    for (Index round {}; round < NUM_ROUNDS; ++round) {
        auto db = Database::open(TEST_PATH, options);
        for (Index i {}; i < ROUND_SIZE; ++i) {
            const auto record = db.read(stob(itr->key));
            ASSERT_NE(record, std::nullopt);
            ASSERT_EQ(record->value, itr->value);
            itr++;
        }
    }
}

TEST_F(DatabaseTests, DataPersists)
{
    run_persistence_test({});
}

TEST_F(DatabaseTests, DataPersistsWhenNotUsingTransactions)
{
    Options options;
    options.use_transactions = false;
    run_persistence_test(options);
}

TEST_F(DatabaseTests, SubsequentAbortsHaveNoEffect)
{
    auto db = Database::open(TEST_PATH, {});
    const auto info = db.get_info();
    const auto records = setup_database_with_committed_records(db, 500);
    for (const auto &[k, v]: records)
        db.erase(stob(k));
    ASSERT_EQ(info.record_count(), 0);
    db.abort();
    ASSERT_EQ(info.record_count(), records.size());
    db.abort();
    ASSERT_EQ(info.record_count(), records.size());
}

TEST(TempDBTests, CannotAbortIfNotUsingTransactions)
{
    auto temp = Database::temp(0x100, false);
    ASSERT_THROW(temp.abort(), std::logic_error);
}

TEST(TempDBTests, FreshDatabaseIsEmpty)
{
    auto temp = Database::temp(0x100);
    auto reader = temp.get_cursor();
    ASSERT_FALSE(reader.has_record());
    ASSERT_EQ(temp.get_info().record_count(), 0);
}

TEST(TempDBTests, CanInsertRecords)
{
    auto temp = Database::temp(0x100);
    const auto records = setup_database_with_committed_records(temp, 500);
    ASSERT_TRUE(database_contains_exact(temp, records));
}

TEST(TempDBTests, AbortClearsRecords)
{
    auto temp = Database::temp(0x100);
    temp.write(stob("a"), stob("1"));
    temp.write(stob("b"), stob("2"));
    temp.write(stob("c"), stob("3"));
    temp.abort();
    ASSERT_TRUE(database_contains_exact(temp, {}));
}

TEST(TempDBTests, AbortKeepsRecordsFromPreviousCommit)
{
    static constexpr auto num_committed = 500;
    auto temp = Database::temp(0x100);
    const auto committed = setup_database_with_committed_records(temp, num_committed);
    temp.write(stob("a"), stob("1"));
    temp.write(stob("b"), stob("2"));
    temp.write(stob("c"), stob("3"));
    temp.abort();
    ASSERT_TRUE(database_contains_exact(temp, committed));
}

TEST_F(DatabaseTests, TestRecovery)
{
    static constexpr Size n = 1000;
    std::vector<Record> records;
    FaultyDatabase db;
    {
        auto faulty = FaultyDatabase::create(0x200);
        setup_database_with_committed_records(*faulty.db, n);
        records = collect_records(*faulty.db);

        // Modify the database by concatenating each value to itself.
        for (const auto &[k, v]: records)
            faulty.db->write(stob(k), stob(v + v));

        try {
            // Fail in the middle of the commit. We fail when flushing the buffer pool, but we have
            // already committed and flushed the WAL. When we reopen the database, we should roll
            // forward.
            faulty.tree_faults.set_write_fault_counter(10);
            faulty.db->commit();
            ADD_FAILURE() << "commit() should have thrown";
        } catch (const IOError&) {

        }
        // Reopen and perform recovery.
        db = faulty.clone();
    }
    for (auto &[k, v]: records)
        v += v;
    ASSERT_TRUE(database_contains_exact(*db.db, records));
}

TEST_F(DatabaseTests, AbortIsReentrant)
{
    static constexpr Size page_size = 0x200;
    static constexpr Size batch_size = 100;
    static constexpr Size num_tries = 5;
    auto db = FaultyDatabase::create(page_size);

    // Cause overflow pages to occupy cache space. This leads to more evictions and writes to the database disk
    // that must be undone in abort().
    RecordGenerator::Parameters param;
    param.mean_value_size = page_size * 3 / 2;

    // This batch of writes should be persisted.
    DatabaseBuilder builder {db.db.get()};
    builder.write_records(batch_size, param);
    const auto records = builder.collect_records();
    {
        // This batch of writes should be undone eventually.
        for (const auto &[k, v]: records)
            db.db->write(stob(k), stob(v + v));

        for (Index i {}; i < num_tries; ++i) {
            try {
                db.tree_faults.set_write_fault_counter(3);
                db.db->abort();
                ADD_FAILURE() << "abort() should have thrown";
            } catch (const IOError&) {
                db.tree_faults.set_write_fault_counter(-1);
            }
        }
        // Perform a successful abort.
        db.db->abort();
    }
    ASSERT_TRUE(database_contains_exact(*db.db, records));
}

TEST_F(DatabaseTests, CanAbortAfterFailingToCommit)
{
    static constexpr Size num_records = 1000;
    auto db = FaultyDatabase::create(0x200);
    DatabaseBuilder builder {db.db.get()};
    builder.write_records(num_records, {});
    const auto records = builder.collect_records();
    {
        for (const auto &[k, v]: records)
            db.db->write(stob(k), stob(v + v));

        try {
            db.tree_faults.set_write_fault_counter(3);
            db.db->commit();
            ADD_FAILURE() << "commit() should have thrown";
        } catch (const IOError&) {
            db.tree_faults.set_write_fault_counter(-1);
        }
        db.db->abort();
    }
    ASSERT_TRUE(database_contains_exact(*db.db, records));
}

TEST_F(DatabaseTests, FindsMinimumRecord)
{
    auto db = Database::open(TEST_PATH, {});
    const auto records = setup_database_with_committed_records(db, 500);
    ASSERT_EQ(db.read_minimum()->value, records.front().value);
}

TEST_F(DatabaseTests, FindsMaximumRecord)
{
    auto db = Database::open(TEST_PATH, {});
    const auto records = setup_database_with_committed_records(db, 500);
    ASSERT_EQ(db.read_maximum()->value, records.back().value);
}

TEST_F(DatabaseTests, DatabaseIsMovable)
{
    auto src = Database::open(TEST_PATH, {});
    [[maybe_unused]] auto dst = std::move(src);
}

class InfoTests: public testing::Test {
public:
    static constexpr Size PAGE_SIZE {0x200};
    static constexpr Size NUM_RECORDS {250};

    InfoTests()
        : db {FaultyDatabase::create(PAGE_SIZE)} {}

    auto add_records()
    {
        DatabaseBuilder builder {db.db.get()};
        builder.write_unique_records(NUM_RECORDS, {});
    }

    FaultyDatabase db;
};

TEST_F(InfoTests, FreshDatabaseIsEmpty)
{
    const auto info = db.db->get_info();
    ASSERT_EQ(info.record_count(), 0);
}

TEST_F(InfoTests, FreshDatabaseHasOnePage)
{
    const auto info = db.db->get_info();
    ASSERT_EQ(info.page_count(), 1);
}

TEST_F(InfoTests, InsertMaximalKey)
{
    const auto info = db.db->get_info();
    const auto max_size = info.maximum_key_size();
    const std::string key(max_size, 'X');
    db.db->write(stob(key), stob(key));
    ASSERT_EQ(db.db->read(stob(key), Ordering::EQ)->value, key);
}

TEST_F(InfoTests, InsertOverMaximalKeyDeathTest)
{
    const auto info = db.db->get_info();
    const auto max_size_plus_1 = info.maximum_key_size() + 1;
    const std::string key(max_size_plus_1, 'X');
    ASSERT_THROW(db.db->write(stob(key), stob(key)), std::invalid_argument);
}

TEST_F(InfoTests, ReportsRecordCountCorrectly)
{
    const auto info = db.db->get_info();
    add_records();
    ASSERT_EQ(info.record_count(), NUM_RECORDS);
}

TEST_F(InfoTests, ReportsOtherInfo)
{
    const auto info = db.db->get_info();
    add_records();
    ASSERT_NE(info.cache_hit_ratio(), 0.0);
    ASSERT_TRUE(info.uses_transactions());
}

class CursorTests: public testing::Test {
public:
    CursorTests()
        : db {FaultyDatabase::create(0x200)}
    {
        static constexpr Size num_records = 250;
        DatabaseBuilder builder {db.db.get()};
        RecordGenerator::Parameters param;
        param.mean_key_size = 16;
        // Use large values and small pages so that the cursor has to move between nodes around a lot.
        param.mean_value_size = 100;
        builder.write_records(num_records, param);
        records = builder.collect_records();
    }

    FaultyDatabase db;
    std::vector<Record> records;
};

TEST_F(CursorTests, CursorDoesNotHaveRecordWhenDatabaseIsEmpty)
{
    auto empty = FaultyDatabase::create(0x200);
    auto cursor = empty.db->get_cursor();
    ASSERT_FALSE(cursor.has_record());
}

TEST_F(CursorTests, ResettingEmptyCursorDoesNothing)
{
    auto empty = FaultyDatabase::create(0x200);
    auto cursor = empty.db->get_cursor();
    cursor.reset();
    ASSERT_FALSE(cursor.has_record());
}

TEST_F(CursorTests, CursorHasRecordWhenDatabaseIsNotEmpty)
{
    auto cursor = db.db->get_cursor();
    ASSERT_TRUE(cursor.has_record());
}

TEST_F(CursorTests, FindsSpecificRecord)
{
    auto dummy = db.db->get_cursor();
    dummy.find_minimum();
    dummy.increment(records.size() / 5);
    const auto record = dummy.record();

    auto cursor = db.db->get_cursor();
    ASSERT_TRUE(cursor.find(stob(record.key)));
    ASSERT_EQ(btos(cursor.key()), record.key);
    ASSERT_EQ(cursor.value(), record.value);
}

TEST_F(CursorTests, FindsMinimumRecord)
{
    auto cursor = db.db->get_cursor();
    cursor.find_minimum();
    ASSERT_TRUE(cursor.is_minimum());
    ASSERT_EQ(btos(cursor.key()), records.front().key);
}

TEST_F(CursorTests, FindsMaximumRecord)
{
    auto cursor = db.db->get_cursor();
    cursor.find_maximum();
    ASSERT_TRUE(cursor.is_maximum());
    ASSERT_EQ(btos(cursor.key()), records.back().key);
}

TEST_F(CursorTests, CannotFindNonexistentRecord)
{
    auto cursor = db.db->get_cursor();
    ASSERT_FALSE(cursor.find(stob("abc")));
    ASSERT_FALSE(cursor.find(stob("123")));
}

TEST_F(CursorTests, IsLeftOnGreaterThanRecordWhenCannotFind)
{
    auto cursor = db.db->get_cursor();
    cursor.find(stob("abc"));
    ASSERT_TRUE(cursor.key() > stob("abc"));
    cursor.find(stob("123"));
    ASSERT_TRUE(cursor.key() > stob("123"));
}

TEST_F(CursorTests, IsLeftOnFirstRecordWhenKeyIsLow)
{
    auto cursor = db.db->get_cursor();
    ASSERT_FALSE(cursor.find(stob("\x01")));
    ASSERT_EQ(btos(cursor.key()), records.front().key);
}

TEST_F(CursorTests, IsLeftOnLastRecordWhenKeyIsHigh)
{
    auto cursor = db.db->get_cursor();
    ASSERT_FALSE(cursor.find(stob("\xFF")));
    ASSERT_EQ(btos(cursor.key()), records.back().key);
}

TEST_F(CursorTests, CanTraverseFullRangeForward)
{
    auto cursor = db.db->get_cursor();
    cursor.find_minimum();
    for (const auto &record: records) {
        ASSERT_TRUE(cursor.record() == record);
        cursor.increment();
    }
}

TEST_F(CursorTests, CanTraversePartialRangeForward)
{
    const auto one_third = records.size() / 3;
    const auto diff = static_cast<ssize_t>(one_third);
    auto cursor = db.db->get_cursor();
    ASSERT_TRUE(cursor.find(stob(records[one_third].key)));
    const auto success = std::all_of(cbegin(records) + diff, cend(records) - diff, [&cursor](const Record &record) {
        const auto is_equal = cursor.record() == record;
        cursor.increment();
        return is_equal;
    });
    ASSERT_TRUE(success);
}

TEST_F(CursorTests, CanTraverseFullRangeBackward)
{
    auto cursor = db.db->get_cursor();
    cursor.find_maximum();
    const auto success = std::all_of(crbegin(records), crend(records), [&cursor](const Record &record) {
        const auto is_equal = cursor.record() == record;
        cursor.decrement();
        return is_equal;
    });
    ASSERT_TRUE(success);
}

TEST_F(CursorTests, CanTraversePartialRangeBackward)
{
    const auto one_third = records.size() / 3;
    const auto diff = static_cast<ssize_t>(one_third);
    auto cursor = db.db->get_cursor();
    auto start = std::crbegin(records) + diff;
    ASSERT_TRUE(cursor.find(stob(start->key)));
    const auto success = std::all_of(start, crend(records) - diff, [&cursor](const Record &record) {
        const auto is_equal = cursor.record() == record;
        cursor.decrement();
        return is_equal;
    });
    ASSERT_TRUE(success);
}

TEST_F(CursorTests, StopsAtEnd)
{
    auto cursor = db.db->get_cursor();
    cursor.find_minimum();
    ASSERT_EQ(cursor.increment(records.size() * 2), records.size() - 1);
    ASSERT_FALSE(cursor.increment());
}

TEST_F(CursorTests, StopsAtBeginning)
{
    auto cursor = db.db->get_cursor();
    cursor.find_maximum();
    ASSERT_EQ(cursor.decrement(records.size() * 2), records.size() - 1);
    ASSERT_FALSE(cursor.decrement());
}

TEST_F(CursorTests, ResettingFreshCursorDoesNothing)
{
    auto cursor = db.db->get_cursor();
    const auto record = cursor.record();
    cursor.reset();
    ASSERT_TRUE(cursor.record() == record);
}

TEST_F(CursorTests, CursorIsMovable)
{
    const auto sink = [](Cursor) {};
    auto src = db.db->get_cursor();
    auto dst = std::move(src);
    sink(std::move(dst));
    sink(db.db->get_cursor());
}

TEST_F(CursorTests, CursorHasNoRecordAfterThrow)
{
    auto cursor = db.db->get_cursor();
    cursor.find_minimum();
    db.tree_faults.set_read_fault_rate(100);
    ASSERT_THROW(while (cursor.increment()) {}, IOError);
    ASSERT_FALSE(cursor.has_record());
}

TEST_F(CursorTests, CursorCanBeResetAfterFailure)
{
    auto cursor = db.db->get_cursor();
    cursor.find_minimum();
    db.tree_faults.set_read_fault_rate(100);
    ASSERT_THROW(while (cursor.increment()) {}, IOError);

    // If we can somehow fix whatever what causing the I/O errors, we can try to call reset().
    // If it succeeds, we should be able to use the cursor like normal.
    db.tree_faults.set_read_fault_rate(0);
    cursor.reset();
    cursor.find_minimum();
    ASSERT_EQ(btos(cursor.key()), records.front().key);
}

} // <anonymous>
