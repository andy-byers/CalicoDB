
#include <array>
#include <filesystem>
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
            db.write(_b(key), _b(key));
        };

        write(K0);
        write(K1);
        write(K2);
    }

    ~DatabaseReadTests() override = default;

    auto read_and_compare(const std::string &key, Comparison comparison, const std::string &target)
    {
        if (const auto record = db.read(_b(key), comparison))
            return record->key == target;
        return false;
    }

    Database db;
};

TEST_F(DatabaseReadTests, ReadsExact)
{
    ASSERT_TRUE(read_and_compare(K0, Comparison::EQ, K0));
    ASSERT_TRUE(read_and_compare(K1, Comparison::EQ, K1));
    ASSERT_TRUE(read_and_compare(K2, Comparison::EQ, K2));
}

TEST_F(DatabaseReadTests, ReadsLessThan)
{
    ASSERT_TRUE(read_and_compare(K0_p1, Comparison::LT, K0));
    ASSERT_TRUE(read_and_compare(K1_p1, Comparison::LT, K1));
    ASSERT_TRUE(read_and_compare(K2_p1, Comparison::LT, K2));
    ASSERT_TRUE(read_and_compare(K1, Comparison::LT, K0));
    ASSERT_TRUE(read_and_compare(K2, Comparison::LT, K1));
}

TEST_F(DatabaseReadTests, ReadsGreaterThan)
{
    ASSERT_TRUE(read_and_compare(K0_m1, Comparison::GT, K0));
    ASSERT_TRUE(read_and_compare(K1_m1, Comparison::GT, K1));
    ASSERT_TRUE(read_and_compare(K2_m1, Comparison::GT, K2));
    ASSERT_TRUE(read_and_compare(K0, Comparison::GT, K1));
    ASSERT_TRUE(read_and_compare(K1, Comparison::GT, K2));
}

TEST_F(DatabaseReadTests, CannotReadNonexistentRecords)
{
    ASSERT_EQ(db.read(_b(K0_m1), Comparison::EQ), std::nullopt);
    ASSERT_EQ(db.read(_b(K1_m1), Comparison::EQ), std::nullopt);
    ASSERT_EQ(db.read(_b(K2_m1), Comparison::EQ), std::nullopt);
}

TEST_F(DatabaseReadTests, CannotReadLessThanMinimum)
{
    ASSERT_EQ(db.read(_b(K0), Comparison::LT), std::nullopt);
    ASSERT_EQ(db.read(_b(K0_m1), Comparison::LT), std::nullopt);
}

TEST_F(DatabaseReadTests, CannotReadGreaterThanMaximum)
{
    ASSERT_EQ(db.read(_b(K2), Comparison::GT), std::nullopt);
    ASSERT_EQ(db.read(_b(K2_p1), Comparison::GT), std::nullopt);
}

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
    db.write(_b("a"), _b("1"));
    db.write(_b("b"), _b("2"));
    db.commit();

    db.write(_b("c"), _b("3"));
    CUB_EXPECT_TRUE(db.erase(_b("a")));
    CUB_EXPECT_TRUE(db.erase(_b("b")));
    db.abort();

    CUB_EXPECT_EQ(db.read(_b("a"), Comparison::EQ)->value, "1");
    CUB_EXPECT_EQ(db.read(_b("b"), Comparison::EQ)->value, "2");
    CUB_EXPECT_EQ(db.read(_b("c"), Comparison::EQ), std::nullopt);

    const auto info = db.get_info();
    CUB_EXPECT_EQ(info.record_count(), 2);
}

TEST_F(DatabaseTests, SubsequentAbortsHaveNoEffect)
{
    auto db = Database::open(TEST_PATH, {});
    const auto info = db.get_info();
    const auto records = setup_database_with_committed_records(db, 500);
    for (const auto &[k, v]: records)
        db.erase(_b(k));
    ASSERT_EQ(info.record_count(), 0);
    db.abort();
    ASSERT_EQ(info.record_count(), records.size());
    db.abort();
    ASSERT_EQ(info.record_count(), records.size());
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
    temp.write(_b("a"), _b("1"));
    temp.write(_b("b"), _b("2"));
    temp.write(_b("c"), _b("3"));
    temp.abort();
    ASSERT_TRUE(database_contains_exact(temp, {}));
}

TEST(TempDBTests, AbortKeepsRecordsFromPreviousCommit)
{
    static constexpr auto num_committed = 500;
    auto temp = Database::temp(0x100);
    const auto committed = setup_database_with_committed_records(temp, num_committed);
    temp.write(_b("a"), _b("1"));
    temp.write(_b("b"), _b("2"));
    temp.write(_b("c"), _b("3"));
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
            faulty.db->write(_b(k), _b(v + v));

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
            db.db->write(_b(k), _b(v + v));

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
            db.db->write(_b(k), _b(v + v));

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

} // <anonymous>