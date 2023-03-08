
#include "calicodb/db.h"
#include "calicodb/table.h"
#include "db_impl.h"
#include "table_impl.h"
#include "unit_tests.h"

namespace calicodb {

static auto print_tables(const TableSet &set)
{
    for (const auto &[table_id, state] : set) {
        std::cerr << "table_id: " << table_id.value << '\n';
        std::cerr << "  iopn: " << state.is_open << '\n';
        std::cerr << "  ttid: " << state.root_id.table_id.value << '\n';
        std::cerr << "  trid: " << state.root_id.page_id.value << '\n';
        std::cerr << "  ckpt: " << state.checkpoint_lsn.value << '\n';
        std::cerr << "  tree: " << state.tree << "\n\n";
    }
}

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

    ~TableTests() override = default;

    auto SetUp() -> void override
    {
        ASSERT_OK(DB::open(options, kFilename, &db));
        ASSERT_OK(TableTests::reopen());
    }

    auto TearDown() -> void override
    {
        delete table;
        ASSERT_OK(db->status());
    }

    virtual auto reopen() -> Status
    {
        delete table;
        table = nullptr;
        print_tables(db_impl(db)->TEST_tables());

        return db->new_table({}, "table", &table);
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
    ASSERT_TRUE(db->new_table({}, "table", &same).is_invalid_argument());
}

TEST_F(TableTests, UncommittedUpdatesAreDiscardedOnTableClose)
{
    ASSERT_OK(table->put("1", "a")); // TODO: Tomorrow sometime: Read all roots into memory on
    ASSERT_OK(table->put("2", "b")); //       startup!!!
    ASSERT_OK(table->checkpoint());
    ASSERT_OK(table->put("3", "c"));
    ASSERT_OK(table->put("4", "d"));

    ASSERT_OK(reopen());

    std::string value;
    ASSERT_OK(table->get("1", &value));
    ASSERT_EQ(value, "a");
    ASSERT_OK(table->get("2", &value));
    ASSERT_EQ(value, "b");

    ASSERT_TRUE(table->get("3", &value).is_not_found());
    ASSERT_TRUE(table->get("4", &value).is_not_found());
}

TEST_F(TableTests, EmptyTableGetsRemovedDuringVacuum)
{
    // Root page of "table" and the pointer map page on page 2 should be removed.
    ASSERT_EQ(db_impl(db)->pager->page_count(), 3);
    ASSERT_OK(db->vacuum());
    ASSERT_EQ(db_impl(db)->pager->page_count(), 1);
}

class TwoTableTests : public TableTests
{
public:
    ~TwoTableTests() override = default;

    auto SetUp() -> void override
    {
        TableTests::SetUp();
        table_1 = table;
        print_tables(db_impl(db)->TEST_tables());

        ASSERT_OK(db->new_table({}, "table_2", &table_2));
    }

    auto TearDown() -> void override
    {
        TableTests::TearDown();
        delete table_2;
        ASSERT_OK(db->status());
    }

    auto reopen() -> Status override
    {
        if (auto s = TableTests::reopen(); !s.is_ok()) {
            return s;
        }
        delete table_2;
        table_2 = nullptr;

        return db->new_table({}, "table_2", &table);
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

TEST_F(TwoTableTests, CheckpointsAreRegistered)
{
    const auto &tables = db_impl(db)->TEST_tables();
    Lsn checkpoints[6];

    ASSERT_FALSE(tables.get(Id {1})->checkpoint_lsn.is_null());
    ASSERT_TRUE(tables.get(Id {2})->checkpoint_lsn.is_null());
    ASSERT_TRUE(tables.get(Id {3})->checkpoint_lsn.is_null());

    ASSERT_OK(table_1->put("a", "1"));
    ASSERT_OK(table_2->put("b", "2"));
    ASSERT_OK(table_1->checkpoint());
    ASSERT_OK(table_2->checkpoint());

    for (int i {}; i < 6; i += 3) {
        checkpoints[i+0] = tables.get(Id {1})->checkpoint_lsn;
        checkpoints[i+1] = tables.get(Id {2})->checkpoint_lsn;
        checkpoints[i+2] = tables.get(Id {3})->checkpoint_lsn;
        ASSERT_NE(checkpoints[i+0], Lsn::null());
        ASSERT_NE(checkpoints[i+1], Lsn::null());
        ASSERT_NE(checkpoints[i+2], Lsn::null());
        ASSERT_LT(checkpoints[i+0], checkpoints[i+1]);
        ASSERT_LT(checkpoints[i+0], checkpoints[i+2]);
        ASSERT_LT(checkpoints[i+1], checkpoints[i+2]);

        reopen();
    }
    // Checkpoints shouldn't change since there were no uncommitted updates when the
    // tables were closed.
    ASSERT_EQ(checkpoints[0], checkpoints[3]);
    ASSERT_EQ(checkpoints[1], checkpoints[4]);
    ASSERT_EQ(checkpoints[2], checkpoints[5]);
}

TEST_F(TwoTableTests, CheckpointsAreAdvancedOnClose)
{
    const auto &tables = db_impl(db)->TEST_tables();
    Lsn before_close[3], after_close[3];

    before_close[0] = tables.get(Id {1})->checkpoint_lsn;
    before_close[1] = tables.get(Id {2})->checkpoint_lsn;
    before_close[2] = tables.get(Id {3})->checkpoint_lsn;

    ASSERT_OK(table_1->put("a", "1"));
    ASSERT_OK(table_2->put("b", "2"));
    reopen();

    after_close[0] = tables.get(Id {1})->checkpoint_lsn;
    after_close[1] = tables.get(Id {2})->checkpoint_lsn;
    after_close[2] = tables.get(Id {3})->checkpoint_lsn;

    ASSERT_EQ(after_close[0], before_close[0]) << "root checkpoint was advanced incorrectly";
    ASSERT_GT(after_close[1], before_close[1]) << "table_1 checkpoint was not advanced";
    ASSERT_GT(after_close[2], before_close[2]) << "table_2 checkpoint was not advanced";
}

TEST_F(TwoTableTests, CheckpointsAreIndependent)
{
    ASSERT_OK(table_1->put("a", "1"));
    ASSERT_OK(table_2->put("b", "2"));
    ASSERT_OK(table_1->checkpoint());

    reopen();

    std::string value;
    ASSERT_OK(table_1->get("a", &value));
    ASSERT_EQ(value, "1");
    ASSERT_TRUE(table_2->get("b", &value).is_not_found());
}

TEST_F(TwoTableTests, RevertsEarlierUpdates)
{
    // Opposite of the last test. Table 2 has WAL records after table 1, but table 1 should still
    // be reverted.
    ASSERT_OK(table_1->put("a", "1"));
    ASSERT_OK(table_2->put("b", "2"));
    ASSERT_OK(table_2->checkpoint());

    reopen();

    std::string value;
    ASSERT_TRUE(table_1->get("a", &value).is_not_found());
    ASSERT_OK(table_2->get("b", &value));
    ASSERT_EQ(value, "2");
}

TEST_F(TwoTableTests, UncommittedUpdatesAreDiscardedOnTableClose)
{
    ASSERT_OK(table_1->put("1", "a"));
    ASSERT_OK(table_1->checkpoint());
    ASSERT_OK(table_1->put("2", "b"));
    ASSERT_OK(table_2->put("3", "c"));
    ASSERT_OK(table_2->checkpoint());
    ASSERT_OK(table_2->put("4", "d"));

    ASSERT_OK(reopen());

    std::string value;
    ASSERT_OK(table_1->get("1", &value));
    ASSERT_EQ(value, "a");
    ASSERT_OK(table_2->get("3", &value));
    ASSERT_EQ(value, "c");

    ASSERT_TRUE(table_1->get("2", &value).is_not_found());
    ASSERT_TRUE(table_2->get("4", &value).is_not_found());
}

TEST_F(TwoTableTests, EmptyTableGetsRemovedDuringVacuum)
{
    ASSERT_OK(table_2->put("k", "v"));

    // Root page of "table_1" should be removed, leaving the database root page, the
    // pointer map page on page 2, and the root page of "table_2".
    ASSERT_EQ(db_impl(db)->pager->page_count(), 4);
    ASSERT_OK(db->vacuum());
    ASSERT_EQ(db_impl(db)->pager->page_count(), 3);
}

} // namespace calicodb