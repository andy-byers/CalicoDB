
#include "calicodb/db.h"
#include "calicodb/table.h"
#include "db_impl.h"
#include "table_impl.h"
#include "unit_tests.h"

namespace calicodb {

static auto db_impl(DB *db) -> DBImpl *
{
    return reinterpret_cast<DBImpl *>(db);
}

static auto table_impl(Table *table) -> TableImpl *
{
    return reinterpret_cast<TableImpl *>(table);
}

class TableTests
    : public InMemoryTest,
      public testing::Test
{
public:
    TableTests()
    {
        options.page_size = kMinPageSize;
        options.cache_size = kMinPageSize * 16;
        options.env = env.get();
    }

    ~TableTests() override
    {
        delete db;
    }

    auto SetUp() -> void override
    {
        ASSERT_OK(TableTests::reopen_db());
        ASSERT_OK(TableTests::reopen_tables());
    }

    auto TearDown() -> void override
    {
        delete table;
        ASSERT_OK(db->status());
    }

    virtual auto reopen_tables() -> Status
    {
        delete table;
        table = nullptr;

        return db->create_table({}, "table", &table);
    }

    virtual auto reopen_db() -> Status
    {
        delete table;
        table = nullptr;

        delete db;
        db = nullptr;

        return DB::open(options, kFilename, &db);
    }

    Options options;
    DB *db {};
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

TEST_F(TableTests, EmptyTableGetsRemovedOnClose)
{
    delete table;
    table = nullptr;
    ASSERT_OK(db->drop_table("table"));

    ASSERT_EQ(db_impl(db)->TEST_tables().get(Id {2}), nullptr);
}

TEST_F(TableTests, EmptyTableRootIsVacuumed)
{
    // Root page of "table" and the pointer map page on page 2 should be removed.
    ASSERT_EQ(db_impl(db)->pager->page_count(), 3);

    delete table;
    table = nullptr;
    ASSERT_OK(db->drop_table("table"));

    // Vacuum gets rid of freelist pages. Root should have been moved to the freelist in
    // the destructor.
    ASSERT_OK(db->vacuum());
    ASSERT_EQ(db_impl(db)->pager->page_count(), 1);
}

TEST_F(TableTests, TableCreationIsPartOfTransaction)
{
    reopen_db();

    ASSERT_NE(db_impl(db)->TEST_tables().get(Id {1}), nullptr);
    ASSERT_EQ(db_impl(db)->TEST_tables().get(Id {2}), nullptr);
}

TEST_F(TableTests, MultipleReadOnlyInstancesAreAllowed)
{
    Table *table_1, *table_2;
    TableOptions table_options {AccessMode::kReadOnly};
    ASSERT_OK(db->create_table(table_options, "t", &table_1));
    ASSERT_OK(db->create_table(table_options, "t", &table_2));
    delete table_1;
    delete table_2;
}

TEST_F(TableTests, OnlyOneWritableInstanceIsAllowed)
{
    Table *table_1, *table_2;
    TableOptions table_options {AccessMode::kReadWrite};
    ASSERT_OK(db->create_table(table_options, "t", &table_1));
    ASSERT_FALSE(db->create_table(table_options, "t", &table_2).is_ok());
    delete table_1;
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
        delete table_2;
        ASSERT_OK(db->status());
    }

    auto reopen_tables() -> Status override
    {
        if (auto s = TableTests::reopen_tables(); !s.is_ok()) {
            return s;
        }
        delete table_2;
        table_2 = nullptr;

        return db->create_table({}, "table_2", &table);
    }

    auto reopen_db() -> Status override
    {
        delete table_2;
        table_2 = nullptr;

        return TableTests::reopen_db();
    }

    Table *table_1 {};
    Table *table_2 {};
};

TEST_F(TwoTableTests, TablesHaveIndependentKeys)
{
    ASSERT_OK(table_1->put("key", "1"));
    ASSERT_OK(table_2->put("key", "2"));

    std::string value;
    ASSERT_OK(table_1->get("key", &value));
    ASSERT_EQ(value, "1");
    ASSERT_OK(table_2->get("key", &value));
    ASSERT_EQ(value, "2");
}

TEST_F(TwoTableTests, DropTable)
{
    ASSERT_OK(table_2->put(
        std::string(10'000, 'A'),
        std::string(10'000, 'Z')));

    delete table_1;
    table_1 = table = nullptr;
    ASSERT_OK(db->drop_table("table"));

    delete table_2;
    table_2 = nullptr;
    ASSERT_OK(db->drop_table("table_2"));

    ASSERT_EQ(db_impl(db)->TEST_tables().get(Id {2}), nullptr) << "table_1 (1 page) was not removed";
    ASSERT_EQ(db_impl(db)->TEST_tables().get(Id {3}), nullptr) << "table_2 (> 1 page) was not removed";

    ASSERT_OK(db->vacuum());
    ASSERT_EQ(db_impl(db)->pager->page_count(), 1);
}

TEST_F(TwoTableTests, TablesCreatedBeforeCheckpointAreRemembered)
{
    ASSERT_OK(db->checkpoint());
    reopen_db();

    const auto &tables = db_impl(db)->TEST_tables();
    ASSERT_NE(tables.get(Id {1}), nullptr) << "cannot locate root table";
    ASSERT_NE(tables.get(Id {2}), nullptr) << "cannot locate first non-root table";
    ASSERT_NE(tables.get(Id {3}), nullptr) << "cannot locate second non-root table";
}

TEST_F(TwoTableTests, TablesCreatedAfterCheckpointAreForgotten)
{
    reopen_db();

    const auto &tables = db_impl(db)->TEST_tables();
    ASSERT_NE(tables.get(Id {1}), nullptr) << "cannot locate root table";
    ASSERT_EQ(tables.get(Id {2}), nullptr) << "first non-root table was not removed";
    ASSERT_EQ(tables.get(Id {3}), nullptr) << "second non-root table was not removed";
}

TEST_F(TwoTableTests, FirstAvailableTableIdIsUsed)
{
    const auto &tables = db_impl(db)->TEST_tables();

    delete table_1;
    table = table_1 = nullptr;
    ASSERT_OK(db->drop_table("table"));

    ASSERT_EQ(tables.get(Id {2}), nullptr);
    ASSERT_OK(db->create_table({}, "\xAB\xCD\xEF", &table_1));

    ASSERT_NE(tables.get(Id {2}), nullptr) << "first table ID was not reused";
}

TEST_F(TwoTableTests, FindExistingTables)
{
    Table *root_table;
    TableOptions root_options {AccessMode::kReadOnly};
    ASSERT_OK(db->create_table(root_options, "calicodb_root", &root_table));

    auto *cursor = root_table->new_cursor();
    cursor->seek_first();
    ASSERT_TRUE(cursor->is_valid());
    ASSERT_EQ(cursor->key(), "table");
    cursor->next();
    ASSERT_TRUE(cursor->is_valid());
    ASSERT_EQ(cursor->key(), "table_2");

    delete table_1;
    table = table_1 = nullptr;
    ASSERT_OK(db->drop_table("table"));

    cursor->seek_first();
    ASSERT_TRUE(cursor->is_valid());
    ASSERT_EQ(cursor->key(), "table_2");

    delete table_2;
    table_2 = nullptr;
    ASSERT_OK(db->drop_table("table_2"));

    cursor->seek_first();
    ASSERT_FALSE(cursor->is_valid());

    delete cursor;
}

} // namespace calicodb