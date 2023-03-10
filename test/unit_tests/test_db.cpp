
#include "db_impl.h"
#include "header.h"
#include "tree.h"
#include "tools.h"
#include "unit_tests.h"
#include "wal.h"
#include <array>
#include <filesystem>
#include <gtest/gtest.h>
#include <vector>

namespace calicodb
{

namespace fs = std::filesystem;

class SetupTests
    : public InMemoryTest,
      public testing::Test
{
};

TEST_F(SetupTests, ReportsInvalidPageSizes)
{
    FileHeader header;
    Options options;

    options.page_size = kMinPageSize / 2;
    ASSERT_TRUE(setup("./test", *env, options, &header).is_invalid_argument());

    options.page_size = kMaxPageSize * 2;
    ASSERT_TRUE(setup("./test", *env, options, &header).is_invalid_argument());

    options.page_size = kMinPageSize + 1;
    ASSERT_TRUE(setup("./test", *env, options, &header).is_invalid_argument());
}

TEST_F(SetupTests, ReportsInvalidCacheSize)
{
    FileHeader header;
    Options options;

    options.cache_size = 1;
    ASSERT_TRUE(setup("./test", *env, options, &header).is_invalid_argument());
}

TEST_F(SetupTests, ReportsInvalidFileHeader)
{
    FileHeader header;
    Options options;

    ASSERT_TRUE(setup("./test", *env, options, &header).is_invalid_argument());
}

TEST(LeakTests, DestroysOwnObjects)
{
    fs::remove_all("__calicodb_test");

    DB *db;
    Table *table;

    ASSERT_OK(DB::open({}, "__calicodb_test", &db));
    ASSERT_OK(db->create_table({}, "test", &table));
    auto *cursor = table->new_cursor();

    delete cursor;
    delete table;
    delete db;

    ASSERT_OK(DB::destroy({}, "__calicodb_test"));
}

TEST(LeakTests, LeavesUserObjects)
{
    Options options;
    options.env = new tools::FakeEnv;
    options.info_log = new tools::StderrLogger;

    DB *db;
    ASSERT_OK(DB::open(options, "__calicodb_test", &db));
    delete db;

    delete options.info_log;
    delete options.env;
}

TEST(BasicDestructionTests, OnlyDeletesCalicoDatabases)
{
    Options options;
    options.env = new tools::FakeEnv;

    // "./test" does not exist.
    ASSERT_TRUE(DB::destroy(options, "./test").is_not_found());
    ASSERT_TRUE(options.env->file_exists("./test").is_not_found());

    // File is too small to read the header.
    Editor *editor;
    ASSERT_OK(options.env->new_editor("./test", &editor));
    ASSERT_TRUE(DB::destroy(options, "./test").is_invalid_argument());
    ASSERT_OK(options.env->file_exists("./test"));

    // Header magic code is incorrect.
    char buffer[FileHeader::kSize];
    FileHeader header;
    header.magic_code = 42;
    header.write(buffer);
    ASSERT_OK(editor->write(Slice {buffer, sizeof(buffer)}, 0));
    ASSERT_TRUE(DB::destroy(options, "./test").is_invalid_argument());

    // Should work, since we just check the magic code.
    header.magic_code = FileHeader::kMagicCode;
    header.write(buffer);
    ASSERT_OK(editor->write(Slice {buffer, sizeof(buffer)}, 0));
    ASSERT_OK(DB::destroy(options, "./test"));

    delete editor;
    delete options.env;
}

TEST(BasicDestructionTests, OnlyDeletesCalicoWals)
{
    Options options;
    options.env = new tools::FakeEnv;
    options.wal_prefix = "./wal-";

    DB *db;
    ASSERT_OK(DB::open(options, "./test", &db));
    delete db;

    // Starts with the WAL prefix of "./wal-", so it is considered a WAL file.
    Editor *editor;
    ASSERT_OK(options.env->new_editor("./wal-1", &editor));
    delete editor;

    ASSERT_OK(options.env->new_editor("./wal_1", &editor));
    delete editor;

    ASSERT_OK(DB::destroy(options, "./test"));
    ASSERT_OK(options.env->file_exists("./wal_1"));
    ASSERT_TRUE(options.env->file_exists("./wal-1").is_not_found());

    delete options.env;
}

class BasicDatabaseTests
    : public OnDiskTest,
      public testing::Test
{
public:
    BasicDatabaseTests()
    {
        options.page_size = 0x200;
        options.cache_size = options.page_size * frame_count;
        options.env = env.get();
    }

    ~BasicDatabaseTests() override
    {
        delete options.info_log;
    }

    [[nodiscard]] static auto db_impl(const DB *db) -> const DBImpl *
    {
        return reinterpret_cast<const DBImpl *>(db);
    }

    std::size_t frame_count {64};
    Options options;
};

TEST_F(BasicDatabaseTests, OpensAndCloses)
{
    DB *db;
    for (std::size_t i {}; i < 3; ++i) {
        ASSERT_OK(DB::open(options, kFilename, &db));
        delete db;
    }
    ASSERT_TRUE(env->file_exists(kFilename).is_ok());
}

TEST_F(BasicDatabaseTests, StatsAreTracked)
{
    DB *db;
    ASSERT_OK(DB::open(options, kFilename, &db));

    Table *table;
    ASSERT_OK(db->create_table({}, "test", &table));

    std::string property;
    ASSERT_TRUE(db->get_property("calicodb.stats", &property));
    ASSERT_TRUE(db->get_property("calicodb.tables", &property));

    ASSERT_EQ(db_impl(db)->record_count(), 0);
    ASSERT_OK(table->put("a", "1"));
    ASSERT_EQ(db_impl(db)->record_count(), 1);
    ASSERT_OK(table->put("a", "A"));
    ASSERT_EQ(db_impl(db)->record_count(), 1);
    ASSERT_OK(table->put("b", "2"));
    ASSERT_EQ(db_impl(db)->record_count(), 2);
    ASSERT_OK(table->erase("a"));
    ASSERT_EQ(db_impl(db)->record_count(), 1);
    ASSERT_OK(table->erase("b"));
    ASSERT_EQ(db_impl(db)->record_count(), 0);

    delete table;
    delete db;
}

TEST_F(BasicDatabaseTests, IsDestroyed)
{
    DB *db;
    ASSERT_OK(DB::open(options, kFilename, &db));
    delete db;

    ASSERT_TRUE(env->file_exists(kFilename).is_ok());
    ASSERT_OK(DB::destroy(options, kFilename));
    ASSERT_TRUE(env->file_exists(kFilename).is_not_found());
}

static auto insert_random_groups(DB &db, std::size_t num_groups, std::size_t group_size)
{
    RecordGenerator generator;
    tools::RandomGenerator random {4 * 1'024 * 1'024};

    Table *table;
    ASSERT_OK(db.create_table({}, "test", &table));

    for (std::size_t iteration {}; iteration < num_groups; ++iteration) {
        const auto records = generator.generate(random, group_size);
        auto itr = cbegin(records);
        ASSERT_OK(db.status());

        for (std::size_t i {}; i < group_size; ++i) {
            ASSERT_OK(table->put(itr->key, itr->value));
            ++itr;
        }
        ASSERT_OK(db.checkpoint());
    }
    delete table;

    dynamic_cast<const DBImpl &>(db).TEST_validate();
}

TEST_F(BasicDatabaseTests, InsertOneGroup)
{
    DB *db;
    ASSERT_OK(DB::open(options, kFilename, &db));
    insert_random_groups(*db, 1, 500);
    delete db;
}

TEST_F(BasicDatabaseTests, InsertMultipleGroups)
{
    DB *db;
    ASSERT_OK(DB::open(options, kFilename, &db));
    insert_random_groups(*db, 5, 500);
    delete db;
}

TEST_F(BasicDatabaseTests, DataPersists)
{
    static constexpr std::size_t NUM_ITERATIONS {5};
    static constexpr std::size_t GROUP_SIZE {10};

    auto s = Status::ok();
    RecordGenerator generator;
    tools::RandomGenerator random {4 * 1'024 * 1'024};

    const auto records = generator.generate(random, GROUP_SIZE * NUM_ITERATIONS);
    auto itr = cbegin(records);
    Table *table;
    DB *db;

    for (std::size_t iteration {}; iteration < NUM_ITERATIONS; ++iteration) {
        ASSERT_OK(DB::open(options, kFilename, &db));
        ASSERT_OK(db->create_table({}, "test", &table));
        ASSERT_OK(db->status());

        for (std::size_t i {}; i < GROUP_SIZE; ++i) {
            ASSERT_OK(table->put(itr->key, itr->value));
            ++itr;
        }
        ASSERT_OK(db->checkpoint());
        delete table;
        delete db;
    }

    ASSERT_OK(DB::open(options, kFilename, &db));
    ASSERT_OK(db->create_table({}, "test", &table));
    for (const auto &[key, value] : records) {
        std::string value_out;
        ASSERT_OK(table->get(key, &value_out));
        ASSERT_EQ(value_out, value);
    }
    delete db;
}

class DbVacuumTests
    : public InMemoryTest,
      public testing::TestWithParam<std::tuple<std::size_t, std::size_t, bool>>
{
public:
    DbVacuumTests()
        : lower_bounds {std::get<0>(GetParam())},
          upper_bounds {std::get<1>(GetParam())},
          reopen {std::get<2>(GetParam())}
    {
        options.page_size = 0x200;
        options.cache_size = 0x200 * 16;
        options.env = env.get();
    }

    std::unordered_map<std::string, std::string> map;
    tools::RandomGenerator random {1'024 * 1'024 * 8};
    DB *db {};
    Options options;
    std::size_t lower_bounds {};
    std::size_t upper_bounds {};
    bool reopen {};
};

TEST_P(DbVacuumTests, SanityCheck)
{
    Table *table;
    ASSERT_OK(DB::open(options, kFilename, &db));
    ASSERT_OK(db->create_table({}, "test", &table));

    for (std::size_t iteration {}; iteration < 4; ++iteration) {
        if (reopen) {
            delete table;
            delete db;
            ASSERT_OK(DB::open(options, kFilename, &db));
            ASSERT_OK(db->create_table({}, "test", &table));
        }
        while (map.size() < upper_bounds) {
            const auto key = random.Generate(10);
            const auto value = random.Generate(options.page_size * 2);
            ASSERT_OK(table->put(key, value));
            map[key.to_string()] = value.to_string();
        }
        while (map.size() > lower_bounds) {
            const auto key = begin(map)->first;
            map.erase(key);
            ASSERT_OK(table->erase(key));
        }
        ASSERT_OK(db->vacuum());
        dynamic_cast<DBImpl &>(*db).TEST_validate();
        ASSERT_OK(db->checkpoint());

        std::size_t i {};
        for (const auto &[key, value] : map) {
            ++i;
            std::string result;
            ASSERT_OK(table->get(key, &result));
            ASSERT_EQ(result, value);
        }
    }
    delete db;
}

INSTANTIATE_TEST_SUITE_P(
    DbVacuumTests,
    DbVacuumTests,
    ::testing::Values(
        std::make_tuple(1, 2, false),
        std::make_tuple(1, 2, true),
        std::make_tuple(10, 20, false),
        std::make_tuple(10, 20, true),
        std::make_tuple(100, 200, false),
        std::make_tuple(100, 200, true),
        std::make_tuple(90, 110, false),
        std::make_tuple(90, 110, true)));

class TestDatabase
{
public:
    explicit TestDatabase(Env &env)
    {
        options.wal_prefix = "./wal-";
        options.page_size = 0x200;
        options.cache_size = 32 * options.page_size;
        options.env = &env;

        EXPECT_OK(reopen());
    }

    virtual ~TestDatabase()
    {
        delete table;
        delete db;
    }

    [[nodiscard]] auto reopen() -> Status
    {
        delete table;
        delete db;
        
        table = nullptr;
        db = nullptr;
        
        CDB_TRY(DB::open(options, "./test", &db));
        return db->create_table({}, "test", &table);
    }

    Options options;
    tools::RandomGenerator random {4 * 1'024 * 1'024};
    std::vector<Record> records;
    DB *db {};
    Table *table {};
};

class DbRevertTests
    : public InMemoryTest,
      public testing::Test
{
protected:
    DbRevertTests()
    {
        db = std::make_unique<TestDatabase>(*env);
    }
    ~DbRevertTests() override = default;

    std::unique_ptr<TestDatabase> db;
};

static auto add_records(TestDatabase &test, std::size_t n)
{
    std::map<std::string, std::string> records;

    for (std::size_t i {}; i < n; ++i) {
        const auto key_size = test.random.Next<std::size_t>(1, test.options.page_size * 2);
        const auto value_size = test.random.Next<std::size_t>(test.options.page_size * 2);
        const auto key = test.random.Generate(key_size).to_string();
        const auto value = test.random.Generate(value_size).to_string();
        EXPECT_OK(test.table->put(key, value));
        records[key] = value;
    }
    return records;
}

static auto expect_contains_records(const Table &table, const std::map<std::string, std::string> &committed)
{
    for (const auto &[key, value] : committed) {
        std::string result;
        ASSERT_OK(table.get(key, &result));
        ASSERT_EQ(result, value);
    }
}

static auto run_revert_test(TestDatabase &db)
{
    const auto committed = add_records(db, 1'000);
    ASSERT_OK(db.db->checkpoint());

    // Hack to make sure the database file is up-to-date.
    (void)reinterpret_cast<DBImpl *>(db.db)->pager->flush({});

    add_records(db, 1'000);
    ASSERT_OK(db.reopen());

    expect_contains_records(*db.table, committed);
}

TEST_F(DbRevertTests, RevertsUncommittedBatch_1)
{
    run_revert_test(*db);
}

TEST_F(DbRevertTests, RevertsUncommittedBatch_2)
{
    add_records(*db, 1'000);
    ASSERT_OK(db->db->checkpoint());
    run_revert_test(*db);
}

TEST_F(DbRevertTests, RevertsUncommittedBatch_3)
{
    run_revert_test(*db);
    add_records(*db, 1'000);
}

TEST_F(DbRevertTests, RevertsUncommittedBatch_4)
{
    add_records(*db, 1'000);
    ASSERT_OK(db->db->checkpoint());
    run_revert_test(*db);
    add_records(*db, 1'000);
}

TEST_F(DbRevertTests, RevertsUncommittedBatch_5)
{
    for (std::size_t i {}; i < 100; ++i) {
        add_records(*db, 100);
        ASSERT_OK(db->db->checkpoint());
    }
    run_revert_test(*db);
    for (std::size_t i {}; i < 100; ++i) {
        add_records(*db, 100);
    }
}

TEST_F(DbRevertTests, RevertsVacuum_1)
{
    const auto committed = add_records(*db, 1'000);
    ASSERT_OK(db->db->checkpoint());

    // Hack to make sure the database file is up-to-date.
    (void)reinterpret_cast<DBImpl *>(db->db)->pager->flush({});

    auto uncommitted = add_records(*db, 1'000);
    for (std::size_t i {}; i < 500; ++i) {
        const auto itr = begin(uncommitted);
        ASSERT_OK(db->table->erase(itr->first));
        uncommitted.erase(itr);
    }
    ASSERT_OK(db->db->vacuum());
    ASSERT_OK(db->reopen());

    expect_contains_records(*db->table, committed);
}

TEST_F(DbRevertTests, RevertsVacuum_2)
{
    auto committed = add_records(*db, 1'000);
    for (std::size_t i {}; i < 500; ++i) {
        const auto itr = begin(committed);
        ASSERT_OK(db->table->erase(itr->first));
        committed.erase(itr);
    }
    ASSERT_OK(db->db->checkpoint());

    (void)reinterpret_cast<DBImpl *>(db->db)->pager->flush({});

    add_records(*db, 1'000);
    ASSERT_OK(db->reopen());

    expect_contains_records(*db->table, committed);
}

TEST_F(DbRevertTests, RevertsVacuum_3)
{
    auto committed = add_records(*db, 1'000);
    for (std::size_t i {}; i < 900; ++i) {
        const auto itr = begin(committed);
        ASSERT_OK(db->table->erase(itr->first));
        committed.erase(itr);
    }
    ASSERT_OK(db->db->checkpoint());

    (void)reinterpret_cast<DBImpl *>(db->db)->pager->flush({});

    auto uncommitted = add_records(*db, 1'000);
    for (std::size_t i {}; i < 500; ++i) {
        const auto itr = begin(uncommitted);
        ASSERT_OK(db->table->erase(itr->first));
        uncommitted.erase(itr);
    }
    ASSERT_OK(db->reopen());

    expect_contains_records(*db->table, committed);
}

class DbRecoveryTests
    : public InMemoryTest,
      public testing::Test
{
protected:
    ~DbRecoveryTests() override = default;
};

TEST_F(DbRecoveryTests, RecoversFirstBatch)
{
    std::unique_ptr<Env> clone;
    std::map<std::string, std::string> snapshot;

    {
        TestDatabase db {*env};
        snapshot = add_records(db, 5);
        ASSERT_OK(db.db->checkpoint());

        // Simulate a crash by cloning the database before cleanup has occurred.
        clone.reset(reinterpret_cast<const tools::FakeEnv &>(*env).clone());

        (void)reinterpret_cast<DBImpl *>(db.db)->pager->flush({});
    }
    // Create a new database from the cloned data. This database will need to roll the WAL forward to become
    // consistent.
    TestDatabase clone_db {*clone};
    ASSERT_OK(clone_db.db->status());
    expect_contains_records(*clone_db.table, snapshot);
}

TEST_F(DbRecoveryTests, RecoversNthBatch)
{
    std::unique_ptr<Env> clone;
    std::map<std::string, std::string> snapshot;

    {
        TestDatabase db {*env};

        for (std::size_t i {}; i < 10; ++i) {
            for (const auto &[k, v] : add_records(db, 100)) {
                snapshot[k] = v;
            }
            ASSERT_OK(db.db->checkpoint());
        }

        clone.reset(dynamic_cast<const tools::FakeEnv &>(*env).clone());

        (void)reinterpret_cast<DBImpl *>(db.db)->pager->flush({});
    }
    TestDatabase clone_db {*clone};
    expect_contains_records(*clone_db.table, snapshot);
}

enum class ErrorTarget {
    kDataWrite,
    kDataRead,
    kWalWrite,
    kWalRead,
};

class DbErrorTests : public testing::TestWithParam<std::size_t>
{
protected:
    DbErrorTests()
    {
        env = std::make_unique<tools::FaultInjectionEnv>();
        db = std::make_unique<TestDatabase>(*env);

        committed = add_records(*db, 5'000);
        EXPECT_OK(db->db->checkpoint());

        env->add_interceptor(
            tools::Interceptor {
                "./test",
                tools::Interceptor::kRead,
                [this] {
                    if (counter++ >= GetParam()) {
                        return special_error();
                    }
                    return Status::ok();
                }});
    }
    ~DbErrorTests() override = default;

    std::unique_ptr<tools::FaultInjectionEnv> env;
    std::unique_ptr<TestDatabase> db;
    std::map<std::string, std::string> committed;
    std::size_t counter {};
};

TEST_P(DbErrorTests, HandlesReadErrorDuringQuery)
{
    for (std::size_t iteration {}; iteration < 2; ++iteration) {
        for (const auto &[k, v] : committed) {
            std::string value;
            const auto s = db->table->get(k, &value);

            if (!s.is_ok()) {
                assert_special_error(s);
                break;
            }
        }
        ASSERT_OK(db->db->status());
        counter = 0;
    }
}

TEST_P(DbErrorTests, HandlesReadErrorDuringIteration)
{
    std::unique_ptr<Cursor> cursor {db->table->new_cursor()};
    cursor->seek_first();
    while (cursor->is_valid()) {
        (void)cursor->key();
        (void)cursor->value();
        cursor->next();
    }
    assert_special_error(cursor->status());
    ASSERT_OK(db->db->status());
    counter = 0;

    cursor->seek_last();
    while (cursor->is_valid()) {
        (void)cursor->key();
        (void)cursor->value();
        cursor->previous();
    }
    assert_special_error(cursor->status());
    ASSERT_OK(db->db->status());
}

TEST_P(DbErrorTests, HandlesReadErrorDuringSeek)
{
    std::unique_ptr<Cursor> cursor {db->table->new_cursor()};

    for (const auto &[k, v] : committed) {
        cursor->seek(k);
        if (!cursor->is_valid()) {
            break;
        }
    }
    assert_special_error(cursor->status());
    ASSERT_OK(db->db->status());
}

INSTANTIATE_TEST_SUITE_P(
    DbErrorTests,
    DbErrorTests,
    ::testing::Values(
        0,
        1,
        10,
        100));

struct ErrorWrapper {
    ErrorTarget target {};
    std::size_t successes {};
};

class DbFatalErrorTests : public testing::TestWithParam<ErrorWrapper>
{
protected:
    DbFatalErrorTests()
    {
        env = std::make_unique<tools::FaultInjectionEnv>();
        db = std::make_unique<TestDatabase>(*env);

        // Make sure all page types are represented in the database.
        committed = add_records(*db, 5'000);
        for (std::size_t i {}; i < 500; ++i) {
            const auto itr = begin(committed);
            EXPECT_OK(db->table->erase(itr->first));
            committed.erase(itr);
        }

        EXPECT_OK(db->db->checkpoint());

        const auto make_interceptor = [this](const auto &prefix, auto type) {
            return tools::Interceptor {prefix, type, [this] {
                                           if (counter++ >= GetParam().successes) {
                                               return special_error();
                                           }
                                           return Status::ok();
                                       }};
        };

        switch (GetParam().target) {
        case ErrorTarget::kDataRead:
            env->add_interceptor(make_interceptor("./test", tools::Interceptor::kRead));
            break;
        case ErrorTarget::kDataWrite:
            env->add_interceptor(make_interceptor("./test", tools::Interceptor::kWrite));
            break;
        case ErrorTarget::kWalRead:
            env->add_interceptor(make_interceptor("./wal-", tools::Interceptor::kRead));
            break;
        case ErrorTarget::kWalWrite:
            env->add_interceptor(make_interceptor("./wal-", tools::Interceptor::kWrite));
            break;
        }
    }

    ~DbFatalErrorTests() override = default;

    std::unique_ptr<tools::FaultInjectionEnv> env;
    std::unique_ptr<TestDatabase> db;
    std::map<std::string, std::string> committed;
    std::size_t counter {};
};

TEST_P(DbFatalErrorTests, ErrorsDuringModificationsAreFatal)
{
    while (db->db->status().is_ok()) {
        auto itr = begin(committed);
        for (std::size_t i {}; i < committed.size() && db->table->erase((itr++)->first).is_ok(); ++i)
            ;
        for (std::size_t i {}; i < committed.size() && db->table->put((itr++)->first, "value").is_ok(); ++i)
            ;
    }
    assert_special_error(db->db->status());
    assert_special_error(db->table->put("key", "value"));
}

TEST_P(DbFatalErrorTests, OperationsAreNotPermittedAfterFatalError)
{
    auto itr = begin(committed);
    while (db->table->erase(itr++->first).is_ok()) {
        ASSERT_NE(itr, end(committed));
    }
    assert_special_error(db->db->status());
    assert_special_error(db->db->checkpoint());
    assert_special_error(db->table->put("key", "value"));
    std::string value;
    assert_special_error(db->table->get("key", &value));
    auto *cursor = db->table->new_cursor();
    assert_special_error(cursor->status());
    delete cursor;
}
//
//TEST_P(DbFatalErrorTests, RecoversFromFatalErrors)
//{
//    auto itr = begin(committed);
//    for (;;) {
//        auto s = db->table->erase(itr++->first);
//        if (!s.is_ok()) {
//            assert_special_error(s);
//            break;
//        }
//        ASSERT_NE(itr, end(committed));
//    }
//    delete db->db;
//    db->db.reset();
//
//    env->clear_interceptors();
//    db->db = std::make_unique<DBImpl>();
//    ASSERT_OK(db->table->put(db->options, "./test"));
//
//    for (const auto &[key, value] : committed) {
//        test_tools::expect_contains(*db->db, key, value);
//    }
//    tools::validate_db(*db->db);
//}

TEST_P(DbFatalErrorTests, VacuumReportsErrors)
{
    assert_special_error(db->db->vacuum());
    assert_special_error(db->db->status());
}

// TODO: This doesn't exercise much of what can go wrong here. Need a test for failure to truncate the file, so the
//       header page count is left incorrect. We should be able to recover from that.
TEST_P(DbFatalErrorTests, RecoversFromVacuumFailure)
{
    assert_special_error(db->db->vacuum());
    delete db->table;
    db->table = nullptr;
    delete db->db;
    db->db = nullptr;

    env->clear_interceptors();
    ASSERT_OK(DB::open(db->options, "./test", &db->db));
    ASSERT_OK(db->db->create_table({}, "test", &db->table));

    for (const auto &[key, value] : committed) {
        std::string result;
        ASSERT_OK(db->table->get(key, &result));
        ASSERT_EQ(result, value);
    }
    tools::validate_db(*db->db);

    std::size_t file_size;
    ASSERT_OK(env->file_size("./test", &file_size));
    ASSERT_EQ(file_size, reinterpret_cast<const DBImpl *>(db->db)->pager->page_count() * db->options.page_size);
}

INSTANTIATE_TEST_SUITE_P(
    DbFatalErrorTests,
    DbFatalErrorTests,
    ::testing::Values(
        ErrorWrapper {ErrorTarget::kDataRead, 0},
        ErrorWrapper {ErrorTarget::kDataRead, 1},
        ErrorWrapper {ErrorTarget::kDataRead, 10},
        ErrorWrapper {ErrorTarget::kDataRead, 100},
        ErrorWrapper {ErrorTarget::kDataWrite, 0},
        ErrorWrapper {ErrorTarget::kDataWrite, 1},
        ErrorWrapper {ErrorTarget::kDataWrite, 10},
        ErrorWrapper {ErrorTarget::kDataWrite, 100},
        ErrorWrapper {ErrorTarget::kWalWrite, 0},
        ErrorWrapper {ErrorTarget::kWalWrite, 1},
        ErrorWrapper {ErrorTarget::kWalWrite, 10},
        ErrorWrapper {ErrorTarget::kWalWrite, 100}));

class DbOpenTests
    : public OnDiskTest,
      public testing::Test
{
protected:
    DbOpenTests()
    {
        options.env = env.get();
        (void)DB::destroy(options, kFilename);
    }

    ~DbOpenTests() override = default;

    Options options;
    DB *db;
};

TEST_F(DbOpenTests, CreatesMissingDb)
{
    options.error_if_exists = false;
    options.create_if_missing = true;
    ASSERT_OK(DB::open(options, kFilename, &db));
    delete db;

    options.create_if_missing = false;
    ASSERT_OK(DB::open(options, kFilename, &db));
    delete db;
}

TEST_F(DbOpenTests, FailsIfMissingDb)
{
    options.create_if_missing = false;
    ASSERT_TRUE(DB::open(options, kFilename, &db).is_invalid_argument());
}

TEST_F(DbOpenTests, FailsIfDbExists)
{
    options.create_if_missing = true;
    options.error_if_exists = true;
    ASSERT_OK(DB::open(options, kFilename, &db));
    delete db;

    options.create_if_missing = false;
    ASSERT_TRUE(DB::open(options, kFilename, &db).is_invalid_argument());
}

class ApiTests : public testing::Test
{
protected:
    static constexpr auto kFilename = "./test";
    static constexpr auto kWalPrefix = "./wal-";

    ApiTests()
    {
        env = std::make_unique<tools::FaultInjectionEnv>();
        options.env = env.get();
        options.wal_prefix = kWalPrefix;
    }

    ~ApiTests() override
    {
        delete table;
        delete db;
    }

    auto SetUp() -> void override
    {
        ApiTests::reopen();
    }

    virtual auto reopen() -> void
    {
        delete table;
        table = nullptr;

        delete db;
        db = nullptr;

        ASSERT_OK(DB::open(options, "./test", &db));
        ASSERT_OK(db->create_table({}, "test", &table));
    }

    std::unique_ptr<tools::FaultInjectionEnv> env;
    Options options;
    Table *table {};
    DB *db {};
};

TEST_F(ApiTests, OnlyReturnsValidProperties)
{
    std::string stats, tables, scratch;
    ASSERT_TRUE(db->get_property("calicodb.stats", &stats));
    ASSERT_TRUE(db->get_property("calicodb.tables", &tables));
    ASSERT_FALSE(db->get_property("Calicodb.tables", &scratch));
    ASSERT_FALSE(db->get_property("calicodb.nonexistent", &scratch));
    ASSERT_FALSE(stats.empty());
    ASSERT_FALSE(tables.empty());
    ASSERT_TRUE(scratch.empty());
}

TEST_F(ApiTests, IsConstCorrect)
{
    ASSERT_OK(table->put("key", "value"));

    const auto *const_table = table;
    std::string value;
    ASSERT_OK(const_table->get("key", &value));

    auto *cursor = const_table->new_cursor();
    cursor->seek_first();

    const auto *const_cursor = cursor;
    ASSERT_TRUE(const_cursor->is_valid());
    ASSERT_OK(const_cursor->status());
    ASSERT_EQ(const_cursor->key(), "key");
    ASSERT_EQ(const_cursor->value(), "value");
    delete const_cursor;

    const auto *const_db = db;
    std::string property;
    ASSERT_TRUE(const_db->get_property("calicodb.tables", &property));
    ASSERT_OK(const_db->status());
}

TEST_F(ApiTests, EmptyKeysAreNotAllowed)
{
    ASSERT_TRUE(table->put("", "value").is_invalid_argument());
}

TEST_F(ApiTests, UncommittedTransactionIsRolledBack)
{
    ASSERT_OK(table->put("a", "1"));
    ASSERT_OK(table->put("b", "2"));
    ASSERT_OK(table->put("c", "3"));
    ASSERT_OK(db->checkpoint());

    ASSERT_OK(table->put("a", "x"));
    ASSERT_OK(table->put("b", "y"));
    ASSERT_OK(table->put("c", "z"));

    reopen();

    ASSERT_OK(calicodb::DB::open(options, "./test", &db));
    auto *cursor = table->new_cursor();
    cursor->seek_first();
    ASSERT_TRUE(cursor->is_valid());
    ASSERT_EQ(cursor->key(), "a");
    ASSERT_EQ(cursor->value(), "1");

    cursor->next();
    ASSERT_TRUE(cursor->is_valid());
    ASSERT_EQ(cursor->key(), "b");
    ASSERT_EQ(cursor->value(), "2");

    cursor->next();
    ASSERT_TRUE(cursor->is_valid());
    ASSERT_EQ(cursor->key(), "c");
    ASSERT_EQ(cursor->value(), "3");

    cursor->next();
    ASSERT_FALSE(cursor->is_valid());
    delete cursor;
}

TEST_F(ApiTests, EmptyTransactionsAreOk)
{
    ASSERT_OK(db->checkpoint());
}

TEST_F(ApiTests, KeysCanBeArbitrarychars)
{
    const std::string key_1 {"\x00\x00", 2};
    const std::string key_2 {"\x00\x01", 2};
    const std::string key_3 {"\x01\x00", 2};

    ASSERT_OK(table->put(key_1, "1"));
    ASSERT_OK(table->put(key_2, "2"));
    ASSERT_OK(table->put(key_3, "3"));
    ASSERT_OK(db->checkpoint());

    auto *cursor = table->new_cursor();
    cursor->seek_first();

    ASSERT_OK(cursor->status());
    ASSERT_EQ(cursor->key(), key_1);
    ASSERT_EQ(cursor->value(), "1");
    cursor->next();

    ASSERT_OK(cursor->status());
    ASSERT_EQ(cursor->key(), key_2);
    ASSERT_EQ(cursor->value(), "2");
    cursor->next();

    ASSERT_OK(cursor->status());
    ASSERT_EQ(cursor->key(), key_3);
    ASSERT_EQ(cursor->value(), "3");
    cursor->next();
    delete cursor;
}

TEST_F(ApiTests, HandlesLargeKeys)
{
    tools::RandomGenerator random {4 * 1'024 * 1'024};

    const auto key_1 = '\x01' + random.Generate(options.page_size * 100).to_string();
    const auto key_2 = '\x02' + random.Generate(options.page_size * 100).to_string();
    const auto key_3 = '\x03' + random.Generate(options.page_size * 100).to_string();

    ASSERT_OK(table->put(key_1, "1"));
    ASSERT_OK(table->put(key_2, "2"));
    ASSERT_OK(table->put(key_3, "3"));
    ASSERT_OK(db->checkpoint());

    auto *cursor = table->new_cursor();
    cursor->seek_first();

    ASSERT_OK(cursor->status());
    ASSERT_EQ(cursor->key(), key_1);
    ASSERT_EQ(cursor->value(), "1");
    cursor->next();

    ASSERT_OK(cursor->status());
    ASSERT_EQ(cursor->key(), key_2);
    ASSERT_EQ(cursor->value(), "2");
    cursor->next();

    ASSERT_OK(cursor->status());
    ASSERT_EQ(cursor->key(), key_3);
    ASSERT_EQ(cursor->value(), "3");
    cursor->next();
    delete cursor;
}

class LargePayloadTests : public ApiTests
{
public:
    [[nodiscard]] auto random_string(std::size_t max_size) const -> std::string
    {
        return random.Generate(random.Next<std::size_t>(1, max_size)).to_string();
    }

    auto run_test(std::size_t max_key_size, std::size_t max_value_size)
    {
        std::unordered_map<std::string, std::string> map;
        for (std::size_t i {}; i < 100; ++i) {
            const auto key = random_string(max_key_size);
            const auto value = random_string(max_value_size);
            ASSERT_OK(table->put(key, value));
        }
        ASSERT_OK(db->checkpoint());

        for (const auto &[key, value] : map) {
            std::string result;
            ASSERT_OK(table->get(key, &result));
            ASSERT_EQ(result, value);
            ASSERT_OK(table->erase(key));
        }
        ASSERT_OK(db->checkpoint());
    }

    tools::RandomGenerator random {4 * 1'024 * 1'024};
};

TEST_F(LargePayloadTests, LargeKeys)
{
    run_test(100 * options.page_size, 100);
}

TEST_F(LargePayloadTests, LargeValues)
{
    run_test(100, 100 * options.page_size);
}

TEST_F(LargePayloadTests, LargePayloads)
{
    run_test(100 * options.page_size, 100 * options.page_size);
}

class CommitFailureTests : public ApiTests
{
protected:
    ~CommitFailureTests() override = default;

    auto SetUp() -> void override
    {
        ApiTests::SetUp();
        ASSERT_OK(table->put("A", "x"));
        ASSERT_OK(table->put("B", "y"));
        ASSERT_OK(table->put("C", "z"));
        ASSERT_OK(db->checkpoint());

        ASSERT_OK(table->put("a", "1"));
        ASSERT_OK(table->put("b", "2"));
        ASSERT_OK(table->put("c", "3"));
    }

    auto reopen() -> void override
    {
        env->clear_interceptors();
        ApiTests::reopen();
    }

    auto assert_contains_exactly(const std::vector<std::string> &keys) -> void
    {
        for (const auto &key : keys) {
            std::string value;
            ASSERT_OK(table->get(key, &value));
        }
        ASSERT_EQ(reinterpret_cast<const DBImpl *>(db)->record_count(), keys.size());
    }
};

TEST_F(CommitFailureTests, WalFlushFailure)
{
    QUICK_INTERCEPTOR(kWalPrefix, tools::Interceptor::kWrite);
    assert_special_error(db->checkpoint());
    assert_special_error(db->status());

    reopen();

    assert_contains_exactly({"A", "B", "C"});
}

class WalPrefixTests
    : public OnDiskTest,
      public testing::Test
{
public:
    WalPrefixTests()
    {
        options.env = env.get();
    }

    Options options;
    DB *db {};
};

TEST_F(WalPrefixTests, WalDirectoryMustExist)
{
    options.wal_prefix = "./nonexistent/wal-";
    ASSERT_TRUE(DB::open(options, kFilename, &db).is_not_found());
}

} // namespace calicodb
