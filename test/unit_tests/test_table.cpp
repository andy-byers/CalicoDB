// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for contributor names.

#include "calicodb/db.h"
#include "db_impl.h"
#include "logging.h"
#include "unit_tests.h"

namespace calicodb
{

class DefaultTableTests
    : public InMemoryTest,
      public testing::Test
{
public:
    DefaultTableTests()
    {
        options.page_size = kMinPageSize;
        options.cache_size = kMinPageSize * 16;
        options.env = env.get();
    }

    ~DefaultTableTests() override
    {
        delete db;
    }

    auto SetUp() -> void override
    {
        ASSERT_OK(DefaultTableTests::reopen_db());
    }

    virtual auto reopen_db() -> Status
    {
        delete db;
        db = nullptr;

        return DB::open(options, kFilename, &db);
    }

    Options options;
    DB *db {};
};

TEST_F(DefaultTableTests, SpecialTableBehavior)
{
    Table *table;
    ASSERT_TRUE(db->create_table({}, "calicodb_root", &table).is_invalid_argument()) << "not allowed to create root table";
    ASSERT_TRUE(db->drop_table(db->default_table()).is_invalid_argument()) << "not allowed to drop default table";
}

TEST_F(DefaultTableTests, RootAndDefaultTablesAreAlwaysOpen)
{
    ASSERT_NE(db_impl(db)->TEST_tables().get(Id {1}), nullptr);
    ASSERT_NE(db_impl(db)->TEST_tables().get(Id {2}), nullptr);

    std::vector<std::string> names;
    ASSERT_OK(db->list_tables(&names));
    ASSERT_TRUE(names.empty());

    std::string v;
    ASSERT_OK(db->put("k", "v"));
    ASSERT_OK(db->get("k", &v));
    ASSERT_EQ(v, "v");
}

TEST_F(DefaultTableTests, DefaultTablePersists)
{
    reopen_db();

    // May cause problems if the default table wasn't registered properly when it was first constructed.
    ASSERT_OK(db->put("k", "v"));
}

TEST_F(DefaultTableTests, RecordInDefaultTablePersists)
{
    ASSERT_OK(db->put("k", "v"));
    ASSERT_OK(db->checkpoint());

    std::string v;
    ASSERT_OK(db->get("k", &v));
    ASSERT_EQ(v, "v");
}

class TableTests : public DefaultTableTests
{
public:
    ~TableTests() override = default;

    auto SetUp() -> void override
    {
        ASSERT_OK(TableTests::reopen_db());
        ASSERT_OK(TableTests::reopen_tables());
    }

    auto TearDown() -> void override
    {
        if (db != nullptr) {
            db->close_table(table);
        }
    }

    virtual auto reopen_tables() -> Status
    {
        db->close_table(table);
        table = nullptr;

        return db->create_table({}, "table", &table);
    }

    auto reopen_db() -> Status override
    {
        if (db != nullptr) {
            db->close_table(table);
            table = nullptr;
        }

        return DefaultTableTests::reopen_db();
    }

    Table *table {};
};

TEST_F(TableTests, TablesAreRegistered)
{
    const auto &tables = db_impl(db)->TEST_tables();
    ASSERT_NE(tables.get(Id {1}), nullptr) << "cannot locate root table";
    ASSERT_NE(tables.get(Id {2}), nullptr) << "cannot locate non-root table";
}

TEST_F(TableTests, TablesMustBeUnique)
{
    Table *same;
    ASSERT_TRUE(db->create_table({}, "table", &same).is_invalid_argument());
}

TEST_F(TableTests, VacuumDroppedTable)
{
    ASSERT_EQ(db_impl(db)->TEST_pager().page_count(), 4);
    ASSERT_OK(db->drop_table(table));
    table = nullptr;

    ASSERT_OK(db->vacuum());
    ASSERT_EQ(db_impl(db)->TEST_pager().page_count(), 3);
}

TEST_F(TableTests, TableCreationIsPartOfTransaction)
{
    reopen_db();

    ASSERT_NE(db_impl(db)->TEST_tables().get(Id {1}), nullptr);
    ASSERT_NE(db_impl(db)->TEST_tables().get(Id {2}), nullptr);
    ASSERT_EQ(db_impl(db)->TEST_tables().get(Id {3}), nullptr);
}

TEST_F(TableTests, TableDestructionIsPartOfTransaction)
{
    ASSERT_OK(db->checkpoint());

    // Checkpoint is needed for the drop_table() to persist after reopen.
    ASSERT_OK(db->drop_table(table));
    table = nullptr;
    ASSERT_OK(db->checkpoint());

    reopen_db();

    ASSERT_NE(db_impl(db)->TEST_tables().get(Id {1}), nullptr);
    ASSERT_NE(db_impl(db)->TEST_tables().get(Id {2}), nullptr);
    ASSERT_EQ(db_impl(db)->TEST_tables().get(Id {3}), nullptr);
}

TEST_F(TableTests, TableCannotBeOpenedTwice)
{
    Table *table_1, *table_2;
    TableOptions table_options {AccessMode::kReadOnly};
    ASSERT_OK(db->create_table(table_options, "t", &table_1));
    ASSERT_FALSE(db->create_table(table_options, "t", &table_2).is_ok());
    db->close_table(table_1);
    table_1 = nullptr;
}

class TwoTableTests : public TableTests
{
public:
    ~TwoTableTests() override = default;

    auto SetUp() -> void override
    {
        TableTests::SetUp();
        table_1 = table;

        ASSERT_OK(db->create_table({}, "table_2", &table_2));
    }

    auto TearDown() -> void override
    {
        TableTests::TearDown();
        db->close_table(table_2);
        ASSERT_OK(db->status());
    }

    auto reopen_tables() -> Status override
    {
        if (auto s = TableTests::reopen_tables(); !s.is_ok()) {
            return s;
        }
        db->close_table(table_2);
        table_2 = nullptr;

        return db->create_table({}, "table_2", &table_2);
    }

    auto reopen_db() -> Status override
    {
        db->close_table(table_2);
        table_2 = nullptr;
        return TableTests::reopen_db();
    }

    Table *table_1 {};
    Table *table_2 {};
};

TEST_F(TwoTableTests, TablesHaveIndependentKeys)
{
    ASSERT_OK(db->put(table_1, "key", "1"));
    ASSERT_OK(db->put(table_2, "key", "2"));

    std::string value;
    ASSERT_OK(db->get(table_1, "key", &value));
    ASSERT_EQ(value, "1");
    ASSERT_OK(db->get(table_2, "key", &value));
    ASSERT_EQ(value, "2");
}

TEST_F(TwoTableTests, DropTable)
{
    ASSERT_OK(db->put(
        table_2,
        std::string(10'000, 'A'),
        std::string(10'000, 'Z')));

    ASSERT_OK(db->drop_table(table_1));
    table = table_1 = nullptr;
    ASSERT_OK(db->drop_table(table_2));
    table_2 = nullptr;

    ASSERT_EQ(db_impl(db)->TEST_tables().get(Id {3}), nullptr) << "table_1 (1 page) was not removed";
    ASSERT_EQ(db_impl(db)->TEST_tables().get(Id {4}), nullptr) << "table_2 (> 1 page) was not removed";

    ASSERT_OK(db->vacuum());
    ASSERT_EQ(db_impl(db)->TEST_pager().page_count(), 3);
}

TEST_F(TwoTableTests, TablesCreatedBeforeCheckpointAreRemembered)
{
    ASSERT_OK(db->checkpoint());
    reopen_db();

    std::vector<std::string> tables;
    ASSERT_OK(db->list_tables(&tables));
    ASSERT_EQ(tables.size(), 2);
    ASSERT_EQ(tables[0], "table");
    ASSERT_EQ(tables[1], "table_2");
}

TEST_F(TwoTableTests, TablesCreatedAfterCheckpointAreForgotten)
{
    reopen_db();

    std::vector<std::string> tables;
    ASSERT_OK(db->list_tables(&tables));
    ASSERT_TRUE(tables.empty());
}

TEST_F(TwoTableTests, FirstAvailableTableIdIsUsed)
{
    const auto &tables = db_impl(db)->TEST_tables();
    ASSERT_OK(db->drop_table(table));
    table = table_1 = nullptr;

    ASSERT_EQ(tables.get(Id {3}), nullptr);
    ASSERT_OK(db->create_table({}, "\xAB\xCD\xEF", &table));

    ASSERT_NE(tables.get(Id {3}), nullptr) << "first table ID was not reused";
}

TEST_F(TwoTableTests, FindExistingTables)
{
    std::vector<std::string> table_names;
    ASSERT_OK(db->list_tables(&table_names));

    // Table names should be in order, since they came from a sequential scan.
    ASSERT_EQ(table_names.size(), 2);
    ASSERT_EQ(table_names[0], "table");
    ASSERT_EQ(table_names[1], "table_2");

    ASSERT_OK(db->drop_table(table));
    table = table_1 = nullptr;
    ASSERT_OK(db->list_tables(&table_names));
    ASSERT_EQ(table_names.size(), 1);
    ASSERT_EQ(table_names[0], "table_2");

    ASSERT_OK(db->drop_table(table_2));
    table_2 = nullptr;
    ASSERT_OK(db->list_tables(&table_names));
    ASSERT_TRUE(table_names.empty());
}

} // namespace calicodb