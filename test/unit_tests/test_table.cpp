// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/db.h"
#include "db_impl.h"
#include "logging.h"
#include "unit_tests.h"

namespace calicodb
{

class TableTestHarness
{
public:
    explicit TableTestHarness(std::size_t n)
        : m_tables(n),
          m_maps(n)
    {
    }

    ~TableTestHarness()
    {
        for (const auto *table : m_tables) {
            delete table;
        }
    }

    auto new_table(Txn &txn, std::size_t i, bool create) -> void
    {
        TableOptions tbopt;
        tbopt.create_if_missing = create;
        tbopt.error_if_exists = create;

        Table *table;
        EXPECT_OK(txn.new_table(tbopt, tools::integral_key(i), table));
        EXPECT_FALSE(m_tables[i]);
        m_tables[i] = table;
    }

    auto table_at(std::size_t i) -> Table *
    {
        return m_tables[i];
    }

    auto close_table(std::size_t i) -> void
    {
        delete m_tables[i];
        m_tables[i] = nullptr;
    }

    auto drop_table(Txn &txn, std::size_t i) -> void
    {
        ASSERT_OK(txn.drop_table(tools::integral_key(i)));
        m_maps[i].clear();
        close_table(i);
    }

    auto reopen_tables(Txn &txn) -> void
    {
        for (std::size_t i = 0; i < m_tables.size(); ++i) {
            if (m_tables[i]) {
                close_table(i);
                new_table(txn, i, false);
            }
        }
    }

    auto update_after_commit() -> void
    {
        m_prev = m_maps;
    }

    auto update_after_rollback() -> void
    {
        m_maps = m_prev;
    }

    auto validate_open_tables() -> void
    {
        ASSERT_EQ(m_tables.size(), m_maps.size()) << "test was incorrectly initialized";
        for (std::size_t i = 0; i < m_tables.size(); ++i) {
            if (m_tables[i]) {
                auto *cur = m_tables[i]->new_cursor();
                auto itr = begin(m_maps[i]);
                while (itr != end(m_maps[i])) {
                    ASSERT_TRUE(cur->is_valid());
                    ASSERT_EQ(itr->first, cur->key().to_string());
                    ASSERT_EQ(itr->second, cur->value().to_string());
                    cur->next();
                }
                ASSERT_FALSE(cur->is_valid());
                delete cur;
            }
        }
    }

private:
    std::vector<Table *> m_tables;
    std::vector<std::map<std::string, std::string>> m_maps;
    std::vector<std::map<std::string, std::string>> m_prev;
};

class TableTests
    : public testing::Test,
      public EnvTestHarness<tools::FakeEnv>
{
protected:
    static constexpr std::size_t kMaxTables = 5;

    ~TableTests() override = default;

    auto SetUp() -> void override
    {
        Options dbopt;
        dbopt.cache_size = kPageSize * kMinFrameCount;
        dbopt.env = &env();
        ASSERT_OK(DB::open(dbopt, kDBFilename, m_db));

        m_harness = std::make_unique<TableTestHarness>(kMaxTables);
    }

    auto begin_with_status(bool write) -> Status
    {
        return m_db->new_txn(write, m_txn);
    }
    auto begin(bool write) -> void
    {
        ASSERT_OK(begin_with_status(write));
    }

    auto commit_with_status() -> Status
    {
        CALICODB_TRY(m_txn->commit());
        m_harness->update_after_commit();
        return Status::ok();
    }
    auto commit() -> void
    {
        ASSERT_OK(commit_with_status());
    }

    auto rollback() -> void
    {
        m_txn->rollback();
        m_harness->update_after_rollback();
    }
    auto finish() -> void
    {
        for (std::size_t i = 0; i < kMaxTables; ++i) {
            m_harness->close_table(i);
        }

        // Uncommitted changes are implicitly rolled back when the transaction
        // is finished.
        m_harness->update_after_rollback();

        delete m_txn;
        m_txn = nullptr;
    }

    DB *m_db = nullptr;
    Txn *m_txn = nullptr;
    std::unique_ptr<TableTestHarness> m_harness;
};

TEST_F(TableTests, NewTables)
{
    begin(true);

    Table *table;
    TableOptions tbopt;
    tbopt.create_if_missing = false;
    tbopt.error_if_exists = false;
    ASSERT_TRUE(m_txn->new_table(tbopt, "table", table).is_invalid_argument());

    tbopt.create_if_missing = true;
    ASSERT_OK(m_txn->new_table(tbopt, "table", table));
    delete table;
    table = nullptr;

    tbopt.error_if_exists = true;
    ASSERT_TRUE(m_txn->new_table(tbopt, "table", table).is_invalid_argument());

    tbopt.create_if_missing = true;
    ASSERT_TRUE(m_txn->new_table(tbopt, "table", table).is_invalid_argument());

    finish();
}

TEST_F(TableTests, TablesHaveUniqueKeyRanges)
{
    begin(true);

    m_harness->new_table(*m_txn, 0, true);
    m_harness->new_table(*m_txn, 1, true);
    m_harness->new_table(*m_txn, 2, true);
    ASSERT_OK(m_harness->table_at(0)->put("*", "a"));
    ASSERT_OK(m_harness->table_at(1)->put("*", "b"));
    ASSERT_OK(m_harness->table_at(2)->put("*", "c"));

    m_harness->reopen_tables(*m_txn);

    std::string value;
    ASSERT_OK(m_harness->table_at(0)->get("*", &value));
    ASSERT_EQ("a", value);
    ASSERT_OK(m_harness->table_at(1)->get("*", &value));
    ASSERT_EQ("b", value);
    ASSERT_OK(m_harness->table_at(2)->get("*", &value));
    ASSERT_EQ("c", value);

    finish();
}

//
// class DefaultTableTests
//    : public EnvTestHarness<tools::FakeEnv>,
//      public testing::Test
//{
// public:
//    DefaultTableTests()
//    {
//        options.page_size = kMinPageSize;
//        options.cache_size = kMinPageSize * 16;
//        options.env = &env();
//    }
//
//    ~DefaultTableTests() override
//    {
//        delete db;
//    }
//
//    auto SetUp() -> void override
//    {
//        ASSERT_OK(DefaultTableTests::reopen_db());
//    }
//
//    virtual auto reopen_db() -> Status
//    {
//        delete db;
//        db = nullptr;
//
//        return DB::open(options, kDBFilename, db);
//    }
//
//    Options options;
//    DB *db = nullptr;
//};
//
// class TableTests : public DefaultTableTests
//{
// public:
//    ~TableTests() override = default;
//
//    auto SetUp() -> void override
//    {
//        ASSERT_OK(TableTests::reopen_db());
//        ASSERT_OK(TableTests::reopen_tables());
//    }
//
//    auto TearDown() -> void override
//    {
//        if (db != nullptr) {
//            db->close_table(table);
//        }
//    }
//
//    virtual auto reopen_tables() -> Status
//    {
//        db->close_table(table);
//        table = nullptr;
//
//        return db->create_table({}, "table", table);
//    }
//
//    auto reopen_db() -> Status override
//    {
//        if (db != nullptr) {
//            db->close_table(table);
//            table = nullptr;
//        }
//
//        return DefaultTableTests::reopen_db();
//    }
//
//    Table *table = nullptr;
//};
//
// TEST_F(TableTests, TablesAreRegistered)
//{
//    const auto &tables = db_impl(db)->TEST_tables();
//    ASSERT_NE(tables.get(Id(1)), nullptr) << "cannot locate root table";
//    ASSERT_NE(tables.get(Id(2)), nullptr) << "cannot locate non-root table";
//}
//
// TEST_F(TableTests, TablesMustBeUnique)
//{
//    Table *same;
//    ASSERT_TRUE(db->create_table({}, "table", same).is_invalid_argument());
//}
//
// TEST_F(TableTests, VacuumDroppedTable)
//{
//    ASSERT_EQ(db_impl(db)->TEST_pager().page_count(), 4);
//    ASSERT_OK(db->drop_table(table));
//    table = nullptr;
//
//    ASSERT_OK(db->vacuum());
//    ASSERT_EQ(db_impl(db)->TEST_pager().page_count(), 3);
//}
//
// TEST_F(TableTests, TableDestructionIsPartOfTransaction)
//{
//    const auto txn = db->begin_txn(TxnOptions());
//    ASSERT_OK(db->drop_table(table));
//    table = nullptr;
//    ASSERT_OK(db->commit_txn(txn));
//
//    reopen_db();
//
//    ASSERT_NE(db_impl(db)->TEST_tables().get(Id(1)), nullptr);
//    ASSERT_NE(db_impl(db)->TEST_tables().get(Id(2)), nullptr);
//    ASSERT_EQ(db_impl(db)->TEST_tables().get(Id(3)), nullptr);
//}
//
// TEST_F(TableTests, TableDestructionIsAtomic)
//{
//    ASSERT_OK(db->drop_table(table));
//    table = nullptr;
//
//    reopen_db();
//
//    ASSERT_NE(db_impl(db)->TEST_tables().get(Id(1)), nullptr);
//    ASSERT_NE(db_impl(db)->TEST_tables().get(Id(2)), nullptr);
//    ASSERT_EQ(db_impl(db)->TEST_tables().get(Id(3)), nullptr);
//}
//
// TEST_F(TableTests, TableCannotBeOpenedTwice)
//{
//    Table *table_1, *table_2;
//    TableOptions table_options{AccessMode::kReadOnly};
//    ASSERT_OK(db->create_table(table_options, "t", table_1));
//    ASSERT_FALSE(db->create_table(table_options, "t", table_2).is_ok());
//    db->close_table(table_1);
//    table_1 = nullptr;
//}
//
// TEST_F(TableTests, RecordsPersist)
//{
//    tools::RandomGenerator random;
//    const auto records_0 = tools::fill_db(*db, random, 1'000);
//    const auto records_1 = tools::fill_db(*db, *table, random, 1'000);
//
//    tools::expect_db_contains(*db, records_0);
//    tools::expect_db_contains(*db, *table, records_1);
//
//    reopen_db();
//    reopen_tables();
//
//    tools::expect_db_contains(*db, records_0);
//    tools::expect_db_contains(*db, *table, records_1);
//}
//
// class TwoTableTests : public TableTests
//{
// public:
//    ~TwoTableTests() override = default;
//
//    auto SetUp() -> void override
//    {
//        TableTests::SetUp();
//        table_1 = table;
//
//        ASSERT_OK(db->create_table({}, "table_2", table_2));
//    }
//
//    auto TearDown() -> void override
//    {
//        TableTests::TearDown();
//        db->close_table(table_2);
//        ASSERT_OK(db->status());
//    }
//
//    auto reopen_tables() -> Status override
//    {
//        if (auto s = TableTests::reopen_tables(); !s.is_ok()) {
//            return s;
//        }
//        table_1 = table;
//        db->close_table(table_2);
//        table_2 = nullptr;
//
//        return db->create_table({}, "table_2", table_2);
//    }
//
//    auto reopen_db() -> Status override
//    {
//        db->close_table(table_2);
//        table_2 = nullptr;
//        return TableTests::reopen_db();
//    }
//
//    Table *table_1 = nullptr;
//    Table *table_2 = nullptr;
//};
//
// TEST_F(TwoTableTests, TablesHaveIndependentKeys)
//{
//    ASSERT_OK(db->put(*table_1, "key", "1"));
//    ASSERT_OK(db->put(*table_2, "key", "2"));
//
//    std::string value;
//    ASSERT_OK(db->get(*table_1, "key", &value));
//    ASSERT_EQ(value, "1");
//    ASSERT_OK(db->get(*table_2, "key", &value));
//    ASSERT_EQ(value, "2");
//}
//
// TEST_F(TwoTableTests, DropTable)
//{
//    ASSERT_OK(db->put(
//        *table_2,
//        std::string(10'000, 'A'),
//        std::string(10'000, 'Z')));
//
//    ASSERT_OK(db->drop_table(table_1));
//    table = table_1 = nullptr;
//    ASSERT_OK(db->drop_table(table_2));
//    table_2 = nullptr;
//
//    ASSERT_EQ(db_impl(db)->TEST_tables().get(Id(3)), nullptr) << "table_1 (1 page) was not removed";
//    ASSERT_EQ(db_impl(db)->TEST_tables().get(Id(4)), nullptr) << "table_2 (> 1 page) was not removed";
//
//    ASSERT_OK(db->vacuum());
//    ASSERT_EQ(db_impl(db)->TEST_pager().page_count(), 3);
//}
//
// TEST_F(TwoTableTests, CreatedTablesPersist)
//{
//    reopen_db();
//
//    std::vector<std::string> tables;
//    ASSERT_OK(db->list_tables(tables));
//    ASSERT_EQ(tables.size(), 2);
//    ASSERT_EQ(tables[0], "table");
//    ASSERT_EQ(tables[1], "table_2");
//}
//
// TEST_F(TwoTableTests, FirstAvailableTableIdIsUsed)
//{
//    const auto &tables = db_impl(db)->TEST_tables();
//    ASSERT_OK(db->drop_table(table));
//    table = table_1 = nullptr;
//
//    ASSERT_EQ(tables.get(Id(3)), nullptr);
//    ASSERT_OK(db->create_table({}, "\xAB\xCD\xEF", table));
//
//    ASSERT_NE(tables.get(Id(3)), nullptr) << "first table ID was not reused";
//}
//
// TEST_F(TwoTableTests, FindExistingTables)
//{
//    std::vector<std::string> table_names;
//    ASSERT_OK(db->list_tables(table_names));
//
//    // Table names should be in order, since they came from a sequential scan.
//    ASSERT_EQ(table_names.size(), 2);
//    ASSERT_EQ(table_names[0], "table");
//    ASSERT_EQ(table_names[1], "table_2");
//
//    ASSERT_OK(db->drop_table(table));
//    table = table_1 = nullptr;
//    ASSERT_OK(db->list_tables(table_names));
//    ASSERT_EQ(table_names.size(), 1);
//    ASSERT_EQ(table_names[0], "table_2");
//
//    ASSERT_OK(db->drop_table(table_2));
//    table_2 = nullptr;
//    ASSERT_OK(db->list_tables(table_names));
//    ASSERT_TRUE(table_names.empty());
//}
//
// TEST_F(TwoTableTests, RecordsPersist)
//{
//    tools::RandomGenerator random;
//    const auto records_0 = tools::fill_db(*db, random, 1'000);
//    const auto records_1 = tools::fill_db(*db, *table_1, random, 1'000);
//    const auto records_2 = tools::fill_db(*db, *table_2, random, 1'000);
//
//    tools::expect_db_contains(*db, records_0);
//    tools::expect_db_contains(*db, *table_1, records_1);
//    tools::expect_db_contains(*db, *table_2, records_2);
//
//    reopen_db();
//    reopen_tables();
//
//    tools::expect_db_contains(*db, records_0);
//    tools::expect_db_contains(*db, *table_1, records_1);
//    tools::expect_db_contains(*db, *table_2, records_2);
//}
//
// class MultiTableVacuumRunner : public EnvTestHarness<tools::FakeEnv>
//{
// public:
//    const std::size_t kRecordCount = 5'000;
//
//    explicit MultiTableVacuumRunner(std::size_t num_tables)
//    {
//        m_options.page_size = kMinPageSize;
//        m_options.cache_size = kMinPageSize * 16;
//        m_options.env = &env();
//        initialize(num_tables, m_options);
//    }
//
//    virtual ~MultiTableVacuumRunner()
//    {
//        for (auto *table : m_tables) {
//            m_db->close_table(table);
//        }
//        delete m_db;
//    }
//
//    auto fill_user_tables(std::size_t n, std::size_t step) -> void
//    {
//        for (std::size_t i = 0; i + step <= n; i += step) {
//            for (std::size_t j = 0; j < m_tables.size(); ++j) {
//                for (const auto &[key, value] : tools::fill_db(*m_db, *m_tables[j], m_random, step)) {
//                    m_records[j].insert_or_assign(key, value);
//                }
//            }
//        }
//    }
//
//    auto erase_from_user_tables(std::size_t n) -> void
//    {
//        for (std::size_t i = 0; i < n; ++i) {
//            for (std::size_t j = 0; j < m_tables.size(); ++j) {
//                const auto itr = begin(m_records[j]);
//                CALICODB_EXPECT_NE(itr, end(m_records[j]));
//                ASSERT_OK(m_db->erase(*m_tables[j], itr->first));
//                m_records[j].erase(itr);
//            }
//        }
//    }
//
//    auto run() -> void
//    {
//        ASSERT_OK(m_db->vacuum());
//        db_impl(m_db)->TEST_validate();
//
//        for (std::size_t i = 0; i < m_tables.size(); ++i) {
//            tools::expect_db_contains(*m_db, *m_tables[i], m_records[i]);
//            m_db->close_table(m_tables[i]);
//        }
//        delete m_db;
//        m_db = nullptr;
//
//        // Make sure all of this stuff can be reverted with the WAL and that the
//        // default table isn't messed up.
//        ASSERT_OK(DB::open(m_options, kDBFilename, m_db));
//        tools::expect_db_contains(*m_db, m_committed);
//
//        // The database would get confused if the root mapping wasn't updated.
//        for (std::size_t i = 0; i < m_tables.size(); ++i) {
//            const auto name = "table_" + tools::integral_key(i);
//            ASSERT_OK(m_db->create_table({}, name, m_tables[i]));
//            m_records[i].clear();
//        }
//        fill_user_tables(kRecordCount, kRecordCount);
//        for (std::size_t i = 0; i < m_tables.size(); ++i) {
//            tools::expect_db_contains(*m_db, *m_tables[i], m_records[i]);
//        }
//
//        db_impl(m_db)->TEST_validate();
//    }
//
// private:
//    auto initialize(std::size_t num_tables, const Options &options) -> void
//    {
//        ASSERT_OK(DB::open(options, kDBFilename, m_db));
//
//        // Create some pages before the 2 user tables.
//        ASSERT_EQ(m_db->begin_txn(TxnOptions()), 1);
//        m_committed = tools::fill_db(*m_db, m_random, kRecordCount);
//        ASSERT_OK(m_db->commit_txn(1));
//
//        ASSERT_EQ(m_db->begin_txn(TxnOptions()), 2);
//        for (std::size_t i = 0; i < num_tables; ++i) {
//            m_tables.emplace_back();
//            m_records.emplace_back();
//            const auto name = "table_" + tools::integral_key(i);
//            ASSERT_OK(m_db->create_table({}, name, m_tables.back()));
//        }
//
//        // Move the filler pages from the default table to the freelist.
//        auto itr = begin(m_committed);
//        for (int i = 0; i < kRecordCount / 2; ++i, ++itr) {
//            ASSERT_OK(m_db->erase(itr->first));
//        }
//    }
//
//    using Map = std::map<std::string, std::string>;
//
//    tools::RandomGenerator m_random;
//    std::vector<Table *> m_tables;
//    std::vector<Map> m_records;
//    Map m_committed;
//    Options m_options;
//    DB *m_db = nullptr;
//};
//
// class MultiTableVacuumTests
//    : public MultiTableVacuumRunner,
//      public testing::TestWithParam<std::size_t>
//{
// public:
//    explicit MultiTableVacuumTests()
//        : MultiTableVacuumRunner{GetParam()}
//    {
//    }
//
//    ~MultiTableVacuumTests() override = default;
//};
//
// TEST_P(MultiTableVacuumTests, EmptyTables)
//{
//    run();
//}
//
// TEST_P(MultiTableVacuumTests, FilledTables)
//{
//    fill_user_tables(15, 15); // kRecordCount, kRecordCount / 2);
//    run();
//}
//
// TEST_P(MultiTableVacuumTests, FilledTablesWithInterleavedPages)
//{
//    fill_user_tables(kRecordCount, 10);
//    run();
//}
//
// TEST_P(MultiTableVacuumTests, PartiallyFilledTables)
//{
//    fill_user_tables(kRecordCount, kRecordCount / 2);
//    erase_from_user_tables(kRecordCount / 2);
//    run();
//}
//
// TEST_P(MultiTableVacuumTests, PartiallyFilledTablesWithInterleavedPages)
//{
//    fill_user_tables(kRecordCount, 10);
//    erase_from_user_tables(kRecordCount / 2);
//    run();
//}
//
// INSTANTIATE_TEST_SUITE_P(
//    MultiTableVacuumTests,
//    MultiTableVacuumTests,
//    ::testing::Values(0, 1, 2, 5, 10));

} // namespace calicodb