
#include "calicodb/db.h"
#include "calicodb/table.h"
#include "db_impl.h"
#include "table_impl.h"
#include "unit_tests.h"

namespace calicodb {

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

    ~TableTests() override = default;

    auto SetUp() -> void override
    {
        ASSERT_OK(DB::open(options, kFilename, &db));
        ASSERT_OK(db->new_table({}, "table", &table));
    }

    auto TearDown() -> void override
    {
        delete table;
        ASSERT_OK(db->status());
    }

    Options options;
    DB *db {};
    Table *table {};
};

TEST_F(TableTests, UncommittedUpdatesAreDiscardedOnClose)
{
    ASSERT_OK(table->put("key", "value"));
    ASSERT_OK(table->checkpoint());
    ASSERT_OK(table->put("1", "a"));
    ASSERT_OK(table->put("2", "b"));
    ASSERT_OK(table->put("3", "c"));

    delete table;
    table = nullptr;
    ASSERT_OK(db->new_table({}, "table", &table));

    std::string value;
    ASSERT_OK(table->get("key", &value));
    ASSERT_EQ(value, "value");
    ASSERT_TRUE(table->get("1", &value).is_not_found());
    ASSERT_TRUE(table->get("2", &value).is_not_found());
    ASSERT_TRUE(table->get("3", &value).is_not_found());
}

class TwoTableTests : public TableTests
{
public:
    ~TwoTableTests() override = default;

    auto SetUp() -> void override
    {
        TableTests::SetUp();
        table_1 = table;
        ASSERT_OK(db->new_table({}, "table_2", &table_2));
    }

    auto TearDown() -> void override
    {
        TableTests::TearDown();
        delete table_2;
        ASSERT_OK(db->status());
    }

    Table *table_1 {};
    Table *table_2 {};
};

TEST_F(TwoTableTests, TablesAreIndependent)
{
    ASSERT_OK(table_1->put("key", "1"));
    ASSERT_OK(table_2->put("key", "2"));

    std::string value;
    ASSERT_OK(table_1->get("key", &value));
    ASSERT_EQ(value, "1");
    ASSERT_OK(table_2->get("key", &value));
    ASSERT_EQ(value, "2");
}

TEST_F(TwoTableTests, CheckpointsAreIndependent)
{
    ASSERT_OK(table_1->put("a", "1"));
    ASSERT_OK(table_2->put("b", "2"));
    ASSERT_OK(table_1->checkpoint());

    delete table_1;
    delete table_2;
    table_1 = nullptr;
    table_2 = nullptr;
    ASSERT_OK(db->new_table({}, "table_1", &table_1));
    ASSERT_OK(db->new_table({}, "table_2", &table_2));

    std::string value;
    ASSERT_OK(table_1->get("a", &value));
    ASSERT_EQ(value, "1");
    ASSERT_TRUE(table_2->get("b", &value).is_not_found());
}

} // namespace calicodb