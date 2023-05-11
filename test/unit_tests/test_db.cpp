// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "db_impl.h"
#include "header.h"
#include "logging.h"
#include "scope_guard.h"
#include "tools.h"
#include "tree.h"
#include "unit_tests.h"
#include "wal.h"
#include <array>
#include <filesystem>
#include <gtest/gtest.h>
#include <vector>

namespace calicodb
{

namespace fs = std::filesystem;

TEST(LeakTests, DestroysOwnObjects)
{
    fs::remove_all("__calicodb_test");

    DB *db;
    Txn *txn;
    Table *table;

    ASSERT_OK(DB::open(Options(), "__calicodb_test", db));
    ASSERT_OK(db->new_txn(true, txn));
    ASSERT_OK(txn->new_table(TableOptions(), "table", table));
    auto *cursor = table->new_cursor();

    delete cursor;
    delete table;
    delete txn;
    delete db;

    ASSERT_OK(DB::destroy({}, "__calicodb_test"));
}

TEST(LeakTests, LeavesUserObjects)
{
    Options options;
    options.env = new tools::FakeEnv;
    options.info_log = nullptr; // TODO

    DB *db;
    ASSERT_OK(DB::open(options, "__calicodb_test", db));
    delete db;

    delete options.info_log;
    delete options.env;
}

TEST(BasicDestructionTests, OnlyDeletesCalicoDatabases)
{
    std::filesystem::remove_all("./testdb");

    Options options;
    options.env = Env::default_env();

    // "./testdb" does not exist.
    ASSERT_TRUE(DB::destroy(options, "./testdb").is_invalid_argument());
    ASSERT_FALSE(options.env->file_exists("./testdb"));

    // File is too small to read the first page.
    File *file;
    ASSERT_OK(options.env->new_file("./testdb", Env::kCreate, file));
    ASSERT_OK(file->write(0, "CalicoDB format"));
    ASSERT_TRUE(DB::destroy(options, "./testdb").is_invalid_argument());
    ASSERT_TRUE(options.env->file_exists("./testdb"));

    // Identifier is incorrect.
    char buffer[FileHeader::kSize];
    FileHeader header;
    header.write(buffer);
    ++buffer[0];
    ASSERT_OK(file->write(0, Slice(buffer, sizeof(buffer))));
    ASSERT_TRUE(DB::destroy(options, "./testdb").is_invalid_argument());

    DB *db;
    std::filesystem::remove_all("./testdb");
    ASSERT_OK(DB::open(options, "./testdb", db));
    ASSERT_OK(DB::destroy(options, "./testdb"));

    delete db;
    delete file;
    delete options.env;
}

TEST(BasicDestructionTests, OnlyDeletesCalicoWals)
{
    Options options;
    options.env = new tools::FakeEnv;
    options.wal_filename = "./wal";

    DB *db;
    ASSERT_OK(DB::open(options, "./test", db));
    delete db;

    // These files are not part of the DB.
    File *file;
    ASSERT_OK(options.env->new_file("./wal_", Env::kCreate, file));
    delete file;
    ASSERT_OK(options.env->new_file("./test.db", Env::kCreate, file));
    delete file;

    ASSERT_OK(DB::destroy(options, "./test"));
    ASSERT_TRUE(options.env->file_exists("./wal_"));
    ASSERT_TRUE(options.env->file_exists("./test.db"));

    delete options.env;
}

class BasicDatabaseTests
    : public EnvTestHarness<PosixEnv>,
      public testing::Test
{
public:
    explicit BasicDatabaseTests()
        : m_testdir("."),
          m_dbname(m_testdir.as_child(kDBFilename))
    {
        options.cache_size = kPageSize * frame_count;
        options.env = &env();
    }

    ~BasicDatabaseTests() override
    {
        delete options.info_log;
    }

    [[nodiscard]] auto db_page_count() -> std::size_t
    {
        std::size_t file_size;
        EXPECT_OK(m_env->file_size(m_dbname, file_size));
        const auto num_pages = file_size / kPageSize;
        EXPECT_EQ(file_size, num_pages * kPageSize)
            << "file size is not a multiple of the page size";
        return num_pages;
    }

    tools::TestDir m_testdir;
    std::string m_dbname;
    std::size_t frame_count = 64;
    Options options;
};

TEST_F(BasicDatabaseTests, OpensAndCloses)
{
    DB *db;
    for (std::size_t i = 0; i < 3; ++i) {
        ASSERT_OK(DB::open(options, m_dbname, db));
        std::size_t file_size;
        ASSERT_OK(env().file_size(m_dbname, file_size));
        ASSERT_EQ(0, file_size);
        delete db;
    }
    ASSERT_TRUE(env().file_exists(m_dbname));

    std::size_t file_size;
    ASSERT_OK(env().file_size(m_dbname, file_size));
    ASSERT_EQ(0, file_size);
}

TEST_F(BasicDatabaseTests, VacuumEmptyDB)
{
    DB *db;
    ASSERT_OK(DB::open(options, m_dbname, db));
    tools::CustomTxnHandler handler = [](auto &txn) {
        return txn.vacuum();
    };
    ASSERT_OK(db->update(handler));
    delete db;
}

TEST_F(BasicDatabaseTests, IsDestroyed)
{
    DB *db;
    ASSERT_OK(DB::open(options, m_dbname, db));
    delete db;

    ASSERT_TRUE(env().file_exists(m_dbname));
    ASSERT_OK(DB::destroy(options, m_dbname));
    ASSERT_FALSE(env().file_exists(m_dbname));
}

TEST_F(BasicDatabaseTests, ClampsBadOptionValues)
{
    const auto open_and_check = [this] {
        DB *db;
        ASSERT_OK(DB::open(options, m_dbname, db));
        delete db;
        ASSERT_OK(DB::destroy(options, m_dbname));
    };

    options.cache_size = kPageSize;
    open_and_check();
}

TEST_F(BasicDatabaseTests, WritesToFiles)
{
    ASSERT_FALSE(env().file_exists(m_dbname));
    ASSERT_FALSE(env().file_exists(m_dbname + kDefaultWalSuffix));
    ASSERT_FALSE(env().file_exists(m_dbname + kDefaultShmSuffix));

    DB *db;
    ASSERT_OK(DB::open(options, m_dbname, db));

    // Database file exists and empty.
    ASSERT_TRUE(env().file_exists(m_dbname));
    auto data = tools::read_file_to_string(env(), m_dbname);
    ASSERT_EQ(0, data.size());

    // WAL and shm are not opened until the first transaction starts.
    ASSERT_FALSE(env().file_exists(m_dbname + kDefaultWalSuffix));
    ASSERT_FALSE(env().file_exists(m_dbname + kDefaultShmSuffix));

    Txn *txn;
    ASSERT_OK(db->new_txn(false, txn));

    // WAL and shm are created when the first transaction starts, even if it is read-only. The shm file
    // is needed to coordinate locks.
    ASSERT_TRUE(env().file_exists(m_dbname + kDefaultWalSuffix));
    ASSERT_TRUE(env().file_exists(m_dbname + kDefaultShmSuffix));

    delete txn;
    ASSERT_OK(db->new_txn(true, txn));

    ASSERT_TRUE(env().file_exists(m_dbname + kDefaultWalSuffix));
    ASSERT_TRUE(env().file_exists(m_dbname + kDefaultShmSuffix));

    std::size_t wal_size;
    ASSERT_OK(env().file_size(m_dbname + kDefaultWalSuffix, wal_size));
    ASSERT_EQ(wal_size, 0);

    Table *table;
    ASSERT_OK(txn->new_table(TableOptions(), "table", table));
    // These writes get put on the same WAL frame as the new table root.
    ASSERT_OK(table->put("k1", "val"));
    ASSERT_OK(table->put("k2", "val"));
    ASSERT_OK(table->put("k3", "val"));
    ASSERT_OK(txn->commit());

    ASSERT_OK(env().file_size(m_dbname + kDefaultWalSuffix, wal_size));
    ASSERT_EQ(wal_size, 32 + (kPageSize + 24) * 3);

    // These writes need to go on a new frame, so that readers can access
    // the version of page 3 at the last commit.
    ASSERT_OK(table->put("k4", "val"));
    ASSERT_OK(table->put("k5", "val"));
    ASSERT_OK(table->put("k6", "val"));
    ASSERT_OK(txn->commit());

    ASSERT_OK(env().file_size(m_dbname + kDefaultWalSuffix, wal_size));
    ASSERT_EQ(wal_size, 32 + (kPageSize + 24) * 4);

    // Transactions that get rolled back shouldn't cause writes to the WAL
    // (unless a page had to be evicted from the page cache, which doesn't
    // happen here).
    ASSERT_OK(table->put("k7", "val"));
    ASSERT_OK(table->put("k8", "val"));
    ASSERT_OK(table->put("k9", "val"));
    txn->rollback();

    ASSERT_OK(env().file_size(m_dbname + kDefaultWalSuffix, wal_size));
    ASSERT_EQ(wal_size, 32 + (kPageSize + 24) * 4);

    delete table;
    delete txn;
    delete db;
}

// CAUTION: PRNG state does not persist between calls.
static auto insert_random_groups(DB &db, std::size_t num_groups, std::size_t group_size)
{
    tools::RandomGenerator random;
    std::map<std::string, std::string> map;
    for (std::size_t iteration = 0; iteration < num_groups; ++iteration) {
        const auto temp = tools::fill_db(db, "table", random, group_size);
        for (const auto &[k, v] : temp) {
            map.insert_or_assign(k, v);
        }
    }
    return map;
}

TEST_F(BasicDatabaseTests, InsertOneGroup)
{
    DB *db;
    ASSERT_OK(DB::open(options, m_dbname, db));
    insert_random_groups(*db, 1, 500);
    delete db;
}

TEST_F(BasicDatabaseTests, InsertMultipleGroups)
{
    DB *db;
    ASSERT_OK(DB::open(options, m_dbname, db));
    insert_random_groups(*db, 5, 500);
    delete db;
}

TEST_F(BasicDatabaseTests, DataPersists)
{
    static constexpr std::size_t kNumIterations = 5;
    static constexpr std::size_t kGroupSize = 10;

    auto s = Status::ok();
    tools::RandomGenerator random(4 * 1'024 * 1'024);

    std::map<std::string, std::string> records;
    DB *db;

    for (std::size_t iteration = 0; iteration < kNumIterations; ++iteration) {
        ASSERT_OK(DB::open(options, m_dbname, db));

        for (const auto &[k, v] : insert_random_groups(*db, 50, kGroupSize)) {
            records.insert_or_assign(k, v);
        }
        delete db;
    }

    ASSERT_OK(DB::open(options, m_dbname, db));
    tools::expect_db_contains(*db, "table", records);
    delete db;
}

TEST_F(BasicDatabaseTests, HandlesMaximumPageSize)
{
    DB *db;
    tools::RandomGenerator random;
    ASSERT_OK(DB::open(options, m_dbname, db));
    const auto records = tools::fill_db(*db, "table", random, 1);
    delete db;

    ASSERT_OK(DB::open(options, m_dbname, db));
    tools::expect_db_contains(*db, "table", records);
    delete db;
}

TEST_F(BasicDatabaseTests, VacuumShrinksDBFileOnCheckpoint)
{
    DB *db;
    ASSERT_OK(DB::open(options, m_dbname, db));
    ASSERT_EQ(db_page_count(), 0);

    Txn *txn;
    tools::RandomGenerator random;
    ASSERT_OK(db->new_txn(true, txn));
    const auto records = tools::fill_db(*txn, "table", random, 1'000);
    ASSERT_OK(txn->commit());
    delete txn;

    delete db;
    db = nullptr;

    const auto saved_page_count = db_page_count();
    ASSERT_GT(saved_page_count, 1)
        << "DB file was not written during checkpoint";

    ASSERT_OK(DB::open(options, m_dbname, db));
    ASSERT_OK(db->new_txn(true, txn));
    Table *table;
    ASSERT_OK(txn->new_table(TableOptions(), "table", table));
    for (const auto &[key, value] : records) {
        ASSERT_OK(table->erase(key));
    }
    delete table;
    ASSERT_OK(txn->drop_table("table"));
    ASSERT_OK(txn->vacuum());
    ASSERT_OK(txn->commit());
    delete txn;

    ASSERT_EQ(saved_page_count, db_page_count())
        << "file should not be modified until checkpoint";

    delete db;

    ASSERT_EQ(db_page_count(), 1)
        << "file was not truncated";
}

class DbVacuumTests
    : public EnvTestHarness<tools::FakeEnv>,
      public testing::TestWithParam<std::tuple<std::size_t, std::size_t, bool>>
{
public:
    DbVacuumTests()
        : m_testdir("."),
          random(1'024 * 1'024 * 16),
          lower_bounds(std::get<0>(GetParam())),
          upper_bounds(std::get<1>(GetParam())),
          reopen(std::get<2>(GetParam()))
    {
        CALICODB_EXPECT_LE(lower_bounds, upper_bounds);
        options.cache_size = 0x200 * 16;
        options.env = &env();
    }

    std::unordered_map<std::string, std::string> map;
    tools::TestDir m_testdir;
    tools::RandomGenerator random;
    DB *db = nullptr;
    Options options;
    std::size_t lower_bounds = 0;
    std::size_t upper_bounds = 0;
    bool reopen = false;
};

TEST_P(DbVacuumTests, SanityCheck)
{
    ASSERT_OK(DB::open(options, m_testdir.as_child(kDBFilename), db));

    for (std::size_t iteration = 0; iteration < 4; ++iteration) {
        if (reopen) {
            delete db;
            ASSERT_OK(DB::open(options, m_testdir.as_child(kDBFilename), db));
        }
        Txn *txn;
        ASSERT_OK(db->new_txn(true, txn));
        Table *table;
        ASSERT_OK(txn->new_table(TableOptions(), "table", table));

        for (std::size_t batch = 0; batch < 4; ++batch) {
            while (map.size() < upper_bounds) {
                const auto key = random.Generate(10);
                const auto value = random.Generate(kPageSize * 2);
                ASSERT_OK(table->put(key, value));
                map.insert_or_assign(key.to_string(), value.to_string());
            }
            while (map.size() > lower_bounds) {
                const auto key = begin(map)->first;
                map.erase(key);
                ASSERT_OK(table->erase(key));
            }
            ASSERT_OK(txn->vacuum());
            reinterpret_cast<TxnImpl *>(txn)->TEST_validate();
        }

        ASSERT_OK(txn->commit());

        std::size_t i = 0;
        for (const auto &[key, value] : map) {
            ++i;
            std::string result;
            ASSERT_OK(table->get(key, &result));
            ASSERT_EQ(result, value);
        }
        delete table;
        delete txn;
    }
    delete db;
}

INSTANTIATE_TEST_SUITE_P(
    DbVacuumTests,
    DbVacuumTests,
    ::testing::Values(
        std::make_tuple(0, 50, false),
        std::make_tuple(0, 50, true),
        std::make_tuple(10, 50, false),
        std::make_tuple(10, 50, true),
        std::make_tuple(0, 2'000, false),
        std::make_tuple(0, 2'000, true),
        std::make_tuple(400, 2'000, false),
        std::make_tuple(400, 2'000, true)));

class TestDatabase
{
public:
    explicit TestDatabase(Env &env)
    {
        options.wal_filename = kWalFilename;
        options.cache_size = 32 * kPageSize;
        options.env = &env;

        EXPECT_OK(reopen());
    }

    virtual ~TestDatabase()
    {
        delete db;
    }

    [[nodiscard]] auto reopen() -> Status
    {
        delete db;

        db = nullptr;

        return DB::open(options, kDBFilename, db);
    }

    Options options;
    tools::RandomGenerator random;
    DB *db = nullptr;
};

class DbRevertTests
    : public EnvTestHarness<tools::FakeEnv>,
      public testing::Test
{
protected:
    DbRevertTests()
    {
        db = std::make_unique<TestDatabase>(env());
    }
    ~DbRevertTests() override = default;

    std::unique_ptr<TestDatabase> db;
};

static auto add_records(TestDatabase &test, std::size_t n, bool commit)
{
    Txn *txn;
    EXPECT_OK(test.db->new_txn(true, txn));
    auto records = tools::fill_db(*txn, "table", test.random, n);
    if (commit) {
        EXPECT_OK(txn->commit());
    }
    delete txn;
    return records;
}

static auto expect_contains_records(DB &db, const std::map<std::string, std::string> &committed)
{
    tools::expect_db_contains(db, "table", committed);
}

static auto run_revert_test(TestDatabase &db)
{
    const auto committed = add_records(db, 1'000, true);
    add_records(db, 1'000, false);

    ASSERT_OK(db.reopen());
    expect_contains_records(*db.db, committed);
}

TEST_F(DbRevertTests, RevertsUncommittedBatch_1)
{
    run_revert_test(*db);
}

TEST_F(DbRevertTests, RevertsUncommittedBatch_2)
{
    add_records(*db, 1'000, true);
    run_revert_test(*db);
}

TEST_F(DbRevertTests, RevertsUncommittedBatch_3)
{
    run_revert_test(*db);
    add_records(*db, 1'000, false);
}

TEST_F(DbRevertTests, RevertsUncommittedBatch_4)
{
    add_records(*db, 1'000, true);
    run_revert_test(*db);
    add_records(*db, 1'000, false);
}

TEST_F(DbRevertTests, RevertsUncommittedBatch_5)
{
    for (std::size_t i = 0; i < 100; ++i) {
        add_records(*db, 100, true);
    }
    run_revert_test(*db);
    for (std::size_t i = 0; i < 100; ++i) {
        add_records(*db, 100, false);
    }
}

struct ErrorWrapper {
    std::string filename;
    tools::SyscallType type;
    std::size_t successes = 0;
};
class DbErrorTests
    : public testing::TestWithParam<ErrorWrapper>,
      public EnvTestHarness<tools::TestEnv>
{
protected:
    using Base = EnvTestHarness<tools::TestEnv>;

    DbErrorTests()
    {
        db = std::make_unique<TestDatabase>(Base::env());
        committed = add_records(*db, 20'000, true);
    }
    ~DbErrorTests() override = default;

    auto set_error() -> void
    {
        env().add_interceptor(
            error.filename,
            tools::Interceptor(
                error.type,
                [this] {
                    if (counter >= 0 && counter++ >= error.successes) {
                        return special_error();
                    }
                    return Status::ok();
                }));
    }

    auto SetUp() -> void override
    {
        Txn *txn_ptr;
        EXPECT_OK(db->db->new_txn(false, txn_ptr));
        txn.reset(txn_ptr);

        Table *table_ptr;
        EXPECT_OK(txn->new_table(TableOptions(), "table", table_ptr));
        table.reset(table_ptr);

        set_error();
    }

    // NOTE: These 3 members cannot be reordered. Their destruction order is important.
    std::unique_ptr<TestDatabase> db;
    std::unique_ptr<Txn> txn;
    std::unique_ptr<Table> table;

    std::map<std::string, std::string> committed;
    ErrorWrapper error{GetParam()};
    int counter = 0;
};

TEST_P(DbErrorTests, HandlesReadErrorDuringQuery)
{
    for (std::size_t iteration = 0; iteration < 2; ++iteration) {
        for (const auto &[k, v] : committed) {
            std::string value;
            const auto s = table->get(k, &value);

            if (!s.is_ok()) {
                assert_special_error(s);
                break;
            }
        }
        ASSERT_OK(txn->status());
        counter = 0;
    }
}

TEST_P(DbErrorTests, HandlesReadErrorDuringIteration)
{
    std::unique_ptr<Cursor> cursor(table->new_cursor());

    cursor->seek_first();
    while (cursor->is_valid()) {
        (void)cursor->key();
        (void)cursor->value();
        cursor->next();
    }
    assert_special_error(cursor->status());
    ASSERT_OK(txn->status());
    counter = 0;

    cursor->seek_last();
    while (cursor->is_valid()) {
        (void)cursor->key();
        (void)cursor->value();
        cursor->previous();
    }
    assert_special_error(cursor->status());
    ASSERT_OK(txn->status());
}

TEST_P(DbErrorTests, HandlesReadErrorDuringSeek)
{
    std::unique_ptr<Cursor> cursor(table->new_cursor());

    for (const auto &[k, v] : committed) {
        cursor->seek(k);
        if (!cursor->is_valid()) {
            break;
        }
    }
    assert_special_error(cursor->status());
    ASSERT_OK(txn->status());
}

INSTANTIATE_TEST_SUITE_P(
    DbErrorTests,
    DbErrorTests,
    ::testing::Values(
        ErrorWrapper{kWalFilename, tools::kSyscallRead, 0},
        ErrorWrapper{kWalFilename, tools::kSyscallRead, 1},
        ErrorWrapper{kWalFilename, tools::kSyscallRead, 10}));

class DbFatalErrorTests : public DbErrorTests
{
protected:
    ~DbFatalErrorTests() override = default;

    auto SetUp() -> void override
    {
        txn.reset();

        Txn *txn_ptr;
        EXPECT_OK(db->db->new_txn(true, txn_ptr));
        txn.reset(txn_ptr);

        Table *table_ptr;
        EXPECT_OK(txn->new_table(TableOptions(), "table", table_ptr));
        table.reset(table_ptr);

        set_error();
    }
};

TEST_P(DbFatalErrorTests, ErrorsDuringModificationsAreFatal)
{
    while (txn->status().is_ok()) {
        auto itr = begin(committed);
        for (std::size_t i = 0; i < committed.size() && table->erase((itr++)->first).is_ok(); ++i)
            ;
        for (std::size_t i = 0; i < committed.size() && table->put((itr++)->first, "value").is_ok(); ++i)
            ;
        assert_special_error(txn->commit());
    }
    assert_special_error(txn->status());
    assert_special_error(table->put("key", "value"));
}

TEST_P(DbFatalErrorTests, OperationsAreNotPermittedAfterFatalError)
{
    auto itr = begin(committed);
    while (table->erase(itr++->first).is_ok()) {
        ASSERT_NE(itr, end(committed));
    }
    assert_special_error(txn->status());
    assert_special_error(txn->commit());
    assert_special_error(table->put("key", "value"));
    std::string value;
    assert_special_error(table->get("key", &value));
    auto *cursor = table->new_cursor();
    assert_special_error(cursor->status());
    delete cursor;
}

// TODO: This doesn't exercise much of what can go wrong here. Need a test for failure to truncate the file, so the
//       header page count is left incorrect. We should be able to recover from that.
TEST_P(DbFatalErrorTests, RecoversFromVacuumFailure)
{
    const auto saved = counter;
    counter = -1;
    auto *cursor = table->new_cursor();
    cursor->seek_first();
    while (cursor->is_valid()) {
        CHECK_OK(table->erase(cursor->key()));
        cursor->seek_first();
    }
    delete cursor;
    counter = saved;

    assert_special_error(txn->vacuum());
    table.reset();
    txn.reset();
    delete db->db;
    db->db = nullptr;

    env().clear_interceptors();
    ASSERT_OK(DB::open(db->options, kDBFilename, db->db));
    Txn *txn_ptr;
    ASSERT_OK(db->db->new_txn(true, txn_ptr));
    txn.reset(txn_ptr);
    Table *table_ptr;
    ASSERT_OK(txn->new_table(TableOptions(), "table", table_ptr));
    table.reset(table_ptr);

    for (const auto &[key, value] : committed) {
        std::string result;
        ASSERT_OK(table->get(key, &result));
        ASSERT_EQ(result, value);
    }
    table.reset();
    txn.reset();
    ASSERT_OK(db->db->checkpoint(true));

    std::size_t file_size;
    ASSERT_OK(env().file_size(kDBFilename, file_size));
    ASSERT_EQ(file_size, db_impl(db->db)->TEST_pager().page_count() * kPageSize);
}

INSTANTIATE_TEST_SUITE_P(
    DbFatalErrorTests,
    DbFatalErrorTests,
    ::testing::Values(
        ErrorWrapper{kWalFilename, tools::kSyscallRead, 1},
        ErrorWrapper{kWalFilename, tools::kSyscallRead, 5},
        ErrorWrapper{kWalFilename, tools::kSyscallWrite, 0},
        ErrorWrapper{kWalFilename, tools::kSyscallWrite, 1},
        ErrorWrapper{kWalFilename, tools::kSyscallWrite, 5}));

class DbOpenTests
    : public EnvTestHarness<PosixEnv>,
      public testing::Test
{
protected:
    explicit DbOpenTests()
    {
        options.env = &env();
    }

    ~DbOpenTests() override = default;

    Options options;
    DB *db;
};

TEST_F(DbOpenTests, CreatesMissingDb)
{
    options.error_if_exists = false;
    options.create_if_missing = true;
    ASSERT_OK(DB::open(options, kDBFilename, db));
    delete db;

    options.create_if_missing = false;
    ASSERT_OK(DB::open(options, kDBFilename, db));
    delete db;
}

TEST_F(DbOpenTests, FailsIfMissingDb)
{
    options.create_if_missing = false;
    ASSERT_TRUE(DB::open(options, kDBFilename, db).is_invalid_argument());
}

TEST_F(DbOpenTests, FailsIfDbExists)
{
    options.create_if_missing = true;
    options.error_if_exists = true;
    ASSERT_OK(DB::open(options, kDBFilename, db));
    delete db;

    options.create_if_missing = false;
    ASSERT_TRUE(DB::open(options, kDBFilename, db).is_invalid_argument());
}

class ApiTests
    : public testing::Test,
      public EnvTestHarness<tools::TestEnv>
{
protected:
    explicit ApiTests()
    {
        options.env = &env();
        options.wal_filename = kWalFilename;
    }

    ~ApiTests() override
    {
        delete table;
        delete txn;
        delete db;
    }

    auto SetUp() -> void override
    {
        ApiTests::reopen(true);
    }

    virtual auto reopen(bool write = true) -> void
    {
        delete table;
        table = nullptr;
        delete txn;
        txn = nullptr;
        delete db;
        db = nullptr;

        ASSERT_OK(DB::open(options, kDBFilename, db));
        ASSERT_OK(db->new_txn(write, txn));
        ASSERT_OK(txn->new_table(TableOptions(), "table", table));
    }

    Options options;
    DB *db = nullptr;
    Txn *txn = nullptr;
    Table *table = nullptr;
};

TEST_F(ApiTests, OnlyReturnsValidProperties)
{
    // Check for existence.
    ASSERT_TRUE(db->get_property("calicodb.stats", nullptr));
    ASSERT_FALSE(db->get_property("Calicodb.stats", nullptr));
    ASSERT_FALSE(db->get_property("calicodb.nonexistent", nullptr));

    std::string stats, scratch;
    ASSERT_TRUE(db->get_property("calicodb.stats", &stats));
    ASSERT_FALSE(db->get_property("Calicodb.stats", &scratch));
    ASSERT_FALSE(db->get_property("calicodb.nonexistent", &scratch));
    ASSERT_FALSE(stats.empty());
    ASSERT_TRUE(scratch.empty());
}

TEST_F(ApiTests, IsConstCorrect)
{
    ASSERT_OK(table->put("key", "value"));
    ASSERT_OK(txn->commit());
    reopen(false);

    ASSERT_OK(txn->status());
    const auto *const_table = table;

    ASSERT_OK(const_table->get("key", nullptr));
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
    ASSERT_TRUE(const_db->get_property("calicodb.stats", &property));
}

TEST_F(ApiTests, EmptyKeysAreNotAllowed)
{
    ASSERT_TRUE(table->put("", "value").is_invalid_argument());
}

TEST_F(ApiTests, EmptyTransactionsAreOk)
{
    ASSERT_OK(txn->commit());
}

TEST_F(ApiTests, OnlyOneTransactionIsAllowed)
{
    Txn *second;
    ASSERT_TRUE(db->new_txn(false, second).is_not_supported());
}

TEST_F(ApiTests, KeysCanBeArbitraryBytes)
{
    const std::string key_1("\x00\x00", 2);
    const std::string key_2("\x00\x01", 2);
    const std::string key_3("\x01\x00", 2);

    ASSERT_OK(table->put(key_1, "1"));
    ASSERT_OK(table->put(key_2, "2"));
    ASSERT_OK(table->put(key_3, "3"));
    ASSERT_OK(txn->commit());

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

TEST_F(ApiTests, CheckIfKeyExists)
{
    ASSERT_TRUE(table->get("k", nullptr).is_not_found());
    ASSERT_OK(table->put("k", "v"));
    ASSERT_OK(table->get("k", nullptr));
}

class LargePayloadTests : public ApiTests
{
public:
    explicit LargePayloadTests()
        : random(kPageSize * 500)
    {
    }

    [[nodiscard]] auto random_string(std::size_t max_size) const -> std::string
    {
        return random.Generate(random.Next(1, max_size)).to_string();
    }

    auto run_test(std::size_t max_key_size, std::size_t max_value_size)
    {
        std::unordered_map<std::string, std::string> map;
        for (std::size_t i = 0; i < 100; ++i) {
            const auto key = random_string(max_key_size);
            const auto value = random_string(max_value_size);
            ASSERT_OK(table->put(key, value));
        }
        ASSERT_OK(txn->commit());

        for (const auto &[key, value] : map) {
            std::string result;
            ASSERT_OK(table->get(key, &result));
            ASSERT_EQ(result, value);
            ASSERT_OK(table->erase(key));
        }
        ASSERT_OK(txn->commit());
    }

    tools::RandomGenerator random;
};

TEST_F(LargePayloadTests, LargeKeys)
{
    run_test(100 * kPageSize, 100);
}

TEST_F(LargePayloadTests, LargeValues)
{
    run_test(100, 100 * kPageSize);
}

TEST_F(LargePayloadTests, LargePayloads)
{
    run_test(100 * kPageSize, 100 * kPageSize);
}

class CommitFailureTests : public ApiTests
{
protected:
    ~CommitFailureTests() override = default;

    auto SetUp() -> void override
    {
        ApiTests::SetUp();

        tools::RandomGenerator random;
        commits[false] = tools::fill_db(*table, random, 5'000);
        ASSERT_OK(txn->commit());

        commits[true] = tools::fill_db(*table, random, 5'678);
        for (const auto &record : commits[false]) {
            commits[true].insert(record);
        }
    }

    auto reopen(bool write = true) -> void override
    {
        env().clear_interceptors();
        ApiTests::reopen(write);
    }

    auto run_test(bool persisted) -> void
    {
        ASSERT_OK(txn->status());
        const auto s = txn->commit();
        ASSERT_EQ(s.is_ok(), persisted);

        reopen();

        for (const auto &[key, value] : commits[persisted]) {
            std::string result;
            ASSERT_OK(table->get(key, &result));
            ASSERT_EQ(value, result);
        }
    }

    std::map<std::string, std::string> commits[2];
};

TEST_F(CommitFailureTests, WalFlushFailure)
{
    QUICK_INTERCEPTOR(kWalFilename, tools::kSyscallWrite);
    run_test(false);
}

class AlternateWalFilenameTests
    : public EnvTestHarness<PosixEnv>,
      public testing::Test
{
public:
    AlternateWalFilenameTests()
    {
        options.env = &env();
    }

    Options options;
    DB *db = nullptr;
};

TEST_F(AlternateWalFilenameTests, WalDirectoryMustExist)
{
    // TODO: It would be nice if this produced an error during DB::open(), rather
    //       than when the first transaction is started.
    options.wal_filename = "./nonexistent/wal";
    ASSERT_OK(DB::open(options, kDBFilename, db));
    Txn *txn;
    const auto s = db->new_txn(false, txn);
    ASSERT_TRUE(s.is_io_error()) << s.to_string();
    delete db;
}

using TestTxnHandler = tools::CustomTxnHandler<std::function<Status(Txn &)>>;
using TestTxnDecision = std::function<bool(std::size_t, std::size_t)>;

class DBConcurrencyTests
    : public testing::TestWithParam<std::tuple<std::size_t, std::size_t>>,
      public ConcurrencyTestHarness<PosixEnv>
{
protected:
    explicit DBConcurrencyTests()
    {
        m_options.cache_size = kMinFrameCount * kPageSize;
        m_options.busy = &m_busy;
        m_options.env = &env();
        m_param.num_processes = std::get<0>(GetParam());
        m_param.num_threads = std::get<1>(GetParam());
    }
    ~DBConcurrencyTests() override = default;

    static auto empty_txn(Txn &) -> Status
    {
        return Status::ok();
    }
    static auto all_readers(std::size_t, std::size_t) -> bool
    {
        return false;
    }
    static auto all_writers(std::size_t, std::size_t) -> bool
    {
        return true;
    }
    static auto single_writer(std::size_t n, std::size_t t) -> bool
    {
        return n + t == 0;
    }
    static auto single_writer_per_process(std::size_t target, std::size_t, std::size_t t) -> bool
    {
        return t == target;
    }
    static auto all_writers_in_single_process(std::size_t target, std::size_t n, std::size_t) -> bool
    {
        return n == target;
    }
    [[nodiscard]] static auto table_get(const Table &table, U64 k, tools::NumericKey<> &out) -> Status
    {
        std::string buffer;
        const tools::NumericKey<> key(k);
        CALICODB_TRY(table.get(key.string(), &buffer));
        out = tools::NumericKey<>(buffer);
        return Status::ok();
    }
    template <
        class IsWriter,
        class Reader,
        class Writer>
    auto run_txn_test(
        std::size_t num_rounds,
        IsWriter is_writer,
        Reader reader,
        Writer writer) -> void
    {
        register_test_callback(
            [=](auto &, auto n, auto t) {
                DB *db;
                tools::CustomTxnHandler read_handler(reader);
                tools::CustomTxnHandler write_handler(writer);
                auto s = busy_wait(nullptr, [&db, this] {
                    return DB::open(m_options, kDBFilename, db);
                });
                for (std::size_t i = 0; s.is_ok() && i < num_rounds; ++i) {
                    if (is_writer(n, t)) {
                        do {
                            // NOTE: If DB::update() returns a status for which Status::is_busy() is true,
                            // the write handler will not have run.
                            s = db->update(write_handler);
                        } while (s.is_busy());
                    } else {
                        s = db->view(read_handler);
                    }
                }
                delete db;
                EXPECT_OK(s);
                return false;
            });
        run_test(m_param);
    }

    ConcurrencyTestParam m_param;
    tools::BusyCounter m_busy;
    Options m_options;
};
// TEST_P(DBConcurrencyTests, Open)
//{
//     register_test_callback(
//         [this](auto &, auto, auto) {
//             DB *db = nullptr;
//             EXPECT_OK(DB::open(m_options, kDBFilename, db));
//             delete db;
//             return false;
//         });
//     run_test(m_param);
// }
// TEST_P(DBConcurrencyTests, StartReaders)
//{
//     register_test_callback(
//         [this](auto &, auto, auto) {
//             DB *db = nullptr;
//             EXPECT_OK(DB::open(m_options, kDBFilename, db));
//             TestTxnHandler handler(empty_txn);
//             EXPECT_OK(db->view(handler));
//             delete db;
//             return false;
//         });
//     run_test(m_param);
// }
// TEST_P(DBConcurrencyTests, StartReadersWhileWriting)
//{
//     register_test_callback(
//         [this](auto &, auto, auto) {
//             DB *db = nullptr;
//             EXPECT_OK(DB::open(m_options, kDBFilename, db));
//             TestTxnHandler handler(empty_txn);
//             EXPECT_OK(db->view(handler));
//             delete db;
//             return false;
//         });
//     run_test(m_param);
// }
// TEST_P(DBConcurrencyTests, ReaderWriterConsistency)
//{
//     static constexpr std::size_t kNumRecords = 500;
//     const auto reader = [](auto &txn, tools::NumericKey<> *value) {
//         TableOptions tbopt;
//         tbopt.create_if_missing = false;
//
//         Table *table = nullptr;
//         auto s = txn.new_table(tbopt, "table", table);
//
//         tools::NumericKey<> v;
//         if (s.is_ok()) {
//             s = table_get(*table, 0, v);
//             if (s.is_ok()) {
//                 for (std::size_t i = 1; i < kNumRecords; ++i) {
//                     tools::NumericKey<> u;
//                     // If the table exists, there should be kNumRecords nonzero values
//                     // written. Each value should be identical.
//                     EXPECT_OK(table_get(*table, i, u));
//                     EXPECT_GT(u.number(), 0);
//                     EXPECT_EQ(u, v) << u.number() << " != " << v.number();
//                 }
//             }
//
//         } else if (s.is_invalid_argument()) {
//             // Table has not been created yet.
//             s = Status::ok();
//         }
//         if (value) {
//             *value = v;
//         }
//         delete table;
//         return s;
//     };
//
//     run_txn_test(
//         1,
//         all_writers,
//         [&reader](Txn &txn) {
//             return reader(txn, nullptr);
//         },
//         [&reader](Txn &txn) {
//             // Determine what value to write next.
//             tools::NumericKey<> value;
//             EXPECT_OK(reader(txn, &value));
//             ++value;
//
//             Table *table;
//             EXPECT_OK(txn.new_table(TableOptions(), "table", table));
//             for (std::size_t i = 0; i < kNumRecords; ++i) {
//                 const tools::NumericKey<> key(i);
//                 EXPECT_OK(table->put(key.string(), value.string()));
//             }
//             delete table;
//             return Status::ok();
//         });
// }
// TEST_P(DBConcurrencyTests, WriterContention)
//{
//     register_test_callback(
//         [this](auto &, auto, auto t) {
//             // Create 1 writer in each process.
//             const auto is_writer = t == 0;
//             Txn *txn = nullptr;
//             DB *db = nullptr;
//
//             EXPECT_OK(DB::open(m_options, kDBFilename, db));
//
//             ScopeGuard guard = [&db, &txn] {
//                 delete txn;
//                 delete db;
//             };
//
//             if (is_writer) {
//                 Status s;
//                 do {
//                     s = db->new_txn(true, txn);
//                 } while (s.is_busy());
//                 EXPECT_OK(s);
//             } else {
//                 EXPECT_OK(db->new_txn(false, txn));
//             }
//
//             Table *table;
//             auto s = txn->new_table(TableOptions(), "table", table);
//             if (s.is_invalid_argument() && !is_writer) {
//                 // Loop until a writer creates the table.
//                 return true;
//             }
//             EXPECT_OK(s);
//             std::string buffer;
//             s = table->get("key", &buffer);
//             EXPECT_TRUE(s.is_ok() || s.is_not_found())
//                 << get_status_name(s) << ": " << s.to_string();
//             Slice value(buffer);
//
//             if (is_writer) {
//                 if (s.is_ok()) {
//                     U64 number;
//                     EXPECT_TRUE(consume_decimal_number(value, &number))
//                         << "corrupted read: " << escape_string(value);
//                     value = number_to_string(number + 1);
//                 } else {
//                     value = "1";
//                 }
//                 EXPECT_OK(table->put("key", value));
//                 EXPECT_OK(txn->commit());
//             }
//             delete table;
//             return false;
//         });
//     run_test(m_param);
//
//     DB *db;
//     ASSERT_OK(DB::open(m_options, kDBFilename, db));
//     tools::CustomTxnHandler handler = [this](auto &txn) {
//         Table *table;
//         TableOptions tbopt;
//         tbopt.create_if_missing = false;
//         EXPECT_OK(txn.new_table(tbopt, "table", table));
//
//         std::string buffer;
//         EXPECT_OK(table->get("key", &buffer));
//         delete table;
//
//         U64 number;
//         Slice value(buffer);
//         EXPECT_TRUE(consume_decimal_number(value, &number));
//         EXPECT_EQ(m_param.num_processes, number);
//         return Status::ok();
//     };
//     ASSERT_OK(db->view(handler));
//     delete db;
// }
// INSTANTIATE_TEST_SUITE_P(
//     DBConcurrencyTests,
//     DBConcurrencyTests,
//     testing::Combine(
//         testing::Values(1, 2, 3, 4, 5),
//         testing::Values(1, 2, 3, 4, 5)),
//     [](const auto &info) {
//         return label_concurrency_test("DBConcurrencyTests", info);
//     });

} // namespace calicodb
