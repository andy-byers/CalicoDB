
#include <array>
#include <filesystem>
#include <thread>
#include <vector>
#include <unordered_set>

#include <gtest/gtest.h>

#include "cub/database.h"
#include "cub/cursor.h"
#include "db/database_impl.h"
#include "db/cursor_impl.h"
#include "pool/interface.h"
#include "cub/common.h"
#include "tree/tree.h"

#include "fakes.h"
#include "tools.h"

#include "file/file.h"

namespace {

using namespace cub;

constexpr auto TEST_PATH = "/tmp/cub_test";

template<class Db> auto database_contains_exact(Db &db, const std::vector<Record> &records)
{
    if (db.get_info().record_count() != records.size())
        return false;

    auto cursor = db.get_cursor();
    for (const auto &[key, value]: records) {
        if (!cursor.find(_b(key)))
            return false;
        if (cursor.key() != _b(key))
            return false;
        if (cursor.value() != value)
            return false;
    }
    return true;
}

template<class Db> auto setup_database_with_committed_records(Db &db, Size n)
{
    insert_random_unique_records(db, n);
    db.commit();
    return collect_records(db);
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

TEST_F(DatabaseTests, DataPersists)
{
    std::vector<Record> records;
    {
        auto db = Database::open(TEST_PATH, {});
        records = setup_database_with_committed_records(db, 500);
    }

    auto db = Database::open(TEST_PATH, {});
    ASSERT_TRUE(database_contains_exact(db, records));
}

TEST_F(DatabaseTests, AbortRestoresState)
{
    auto db = Database::open(TEST_PATH, {});
    db.insert(_b("a"), _b("1"));
    db.insert(_b("b"), _b("2"));
    db.commit();

    db.insert(_b("c"), _b("3"));
    CUB_EXPECT_TRUE(db.remove(_b("a")));
    CUB_EXPECT_TRUE(db.remove(_b("b")));
    db.abort();

    CUB_EXPECT_EQ(db.lookup(_b("a"), true)->value, "1");
    CUB_EXPECT_EQ(db.lookup(_b("b"), true)->value, "2");
    CUB_EXPECT_EQ(db.lookup(_b("c"), true), std::nullopt);

    const auto info = db.get_info();
    CUB_EXPECT_EQ(info.record_count(), 2);
}

TEST_F(DatabaseTests, SubsequentAbortsHaveNoEffect)
{
    std::vector<Record> records;
    auto db = Database::open(TEST_PATH, {});
    records = setup_database_with_committed_records(db, 500);
    insert_random_unique_records(db, 100);

    db.abort();
    ASSERT_TRUE(database_contains_exact(db, records));
    db.abort();
    ASSERT_TRUE(database_contains_exact(db, records));
}

TEST(TempDBTests, FreshDatabaseIsEmpty)
{
    auto temp = Database::temp(0x100);
    auto records = collect_records(temp);
    ASSERT_TRUE(collect_records(temp).empty());
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
    insert_random_unique_records(temp, 500);
    temp.abort();
    ASSERT_TRUE(database_contains_exact(temp, {}));
}

TEST(TempDBTests, AbortKeepsRecordsFromPreviousCommit)
{
    static constexpr auto num_committed = 500;
    auto temp = Database::temp(0x100);
    const auto committed = setup_database_with_committed_records(temp, num_committed);
    insert_random_unique_records(temp, num_committed);
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
        insert_random_records(*faulty.db, n, {});
        faulty.db->commit();
        records = collect_records(*faulty.db);
        insert_random_records(*faulty.db, n, {});

        try {
            // Fail in the middle of the commit.
            faulty.tree_faults.set_write_fault_counter(10);
            faulty.db->commit();
            ADD_FAILURE() << "commit() should have thrown";
        } catch (const IOError&) {

        }
        db = faulty.clone();
    }
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
    param.min_value_size = page_size;
    param.max_value_size = page_size * 2;

    // This batch of inserts should be persisted.
    insert_random_records(*db.db, batch_size, param);
    const auto records = collect_records(*db.db);
    db.db->commit();

    // This batch of inserts should be undone.
    insert_random_records(*db.db, batch_size, param);

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
    ASSERT_TRUE(database_contains_exact(*db.db, records));
}

TEST_F(DatabaseTests, CanAbortAfterFailingToCommit)
{
    static constexpr Size num_records = 1000;
    Random random {0};
    auto db = FaultyDatabase::create(0x200);
    insert_random_records(*db.db, num_records, {});
    const auto records = collect_records(*db.db);

    try {
        db.tree_faults.set_write_fault_counter(3);
        db.db->commit();
        ADD_FAILURE() << "commit() should have thrown";
    } catch (const IOError&) {
        db.tree_faults.set_write_fault_counter(-1);
    }
    db.db->abort();
    ASSERT_TRUE(database_contains_exact(*db.db, {}));
}

} // <anonymous>