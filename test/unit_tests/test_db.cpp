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
    ASSERT_OK(db->start(true, txn));
    ASSERT_OK(txn->new_table(TableOptions(), "test", table));
    auto *cursor = table->new_cursor();

    delete cursor;
    delete table;

    db->finish(txn);

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
        delete db;
    }
    ASSERT_TRUE(env().file_exists(m_dbname));
}

TEST_F(BasicDatabaseTests, InitialState)
{
    DB *db;
    ASSERT_OK(DB::open(options, m_dbname, db));
    delete db;

    const auto file = tools::read_file_to_string(env(), m_dbname);
    ASSERT_EQ(file.size(), kPageSize)
        << "DB should have allocated 1 page (the root page)";

    FileHeader header;
    ASSERT_TRUE(header.read(file.data()))
        << "version identifier mismatch";
    ASSERT_EQ(header.page_count, 1);
    ASSERT_EQ(header.freelist_head, 0);
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
    options.cache_size = 1 << 31;
    open_and_check();
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
    ASSERT_EQ(db_page_count(), 1);

    Txn *txn;
    tools::RandomGenerator random;
    ASSERT_OK(db->start(true, txn));
    const auto records = tools::fill_db(*txn, "table", random, 1'000);
    ASSERT_OK(txn->commit());
    db->finish(txn);

    delete db;
    db = nullptr;

    const auto saved_page_count = db_page_count();
    ASSERT_GT(saved_page_count, 1)
        << "DB file was not written during checkpoint";

    ASSERT_OK(DB::open(options, m_dbname, db));
    ASSERT_OK(db->start(true, txn));
    Table *table;
    ASSERT_OK(txn->new_table(TableOptions(), "table", table));
    for (const auto &[key, value] : records) {
        ASSERT_OK(table->erase(key));
    }
    delete table;
    ASSERT_OK(txn->drop_table("table"));
    ASSERT_OK(txn->vacuum());
    ASSERT_OK(txn->commit());
    db->finish(txn);

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
        ASSERT_OK(db->start(true, txn));
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
        db->finish(txn);
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
        options.wal_filename = "./wal";
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

        return DB::open(options, "./test", db);
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
    EXPECT_OK(test.db->start(true, txn));
    auto records = tools::fill_db(*txn, "table", test.random, n);
    if (commit) {
        EXPECT_OK(txn->commit());
    }
    test.db->finish(txn);
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

// TEST_F(DbRevertTests, RevertsVacuum_1)
//{
//     const auto committed = add_records(*db, 1'000, true);
//
//     auto uncommitted = add_records(*db, 1'000, false);
//     for (std::size_t i = 0; i < 500; ++i) {
//         const auto itr = begin(uncommitted);
//         ASSERT_OK(db->db->erase(itr->first));
//         uncommitted.erase(itr);
//     }
//     ASSERT_OK(db->db->vacuum());
//     ASSERT_OK(db->reopen());
//
//     expect_contains_records(*db->db, committed);
// }

// TEST_F(DbRevertTests, RevertsVacuum_2)
//{
//     ASSERT_EQ(db->db->begin_txn(TxnOptions()), 1);
//     auto committed = add_records(*db, 1'000);
//     for (std::size_t i = 0; i < 500; ++i) {
//         const auto itr = begin(committed);
//         ASSERT_OK(db->db->erase(itr->first));
//         committed.erase(itr);
//     }
//     ASSERT_OK(db->db->commit_txn(1));
//
//     ASSERT_EQ(db->db->begin_txn(TxnOptions()), 2);
//     add_records(*db, 1'000);
//     ASSERT_OK(db->reopen());
//
//     expect_contains_records(*db->db, committed);
// }
//
// TEST_F(DbRevertTests, RevertsVacuum_3)
//{
//     ASSERT_EQ(db->db->begin_txn(TxnOptions()), 1);
//     auto committed = add_records(*db, 1'000);
//     for (std::size_t i = 0; i < 900; ++i) {
//         const auto itr = begin(committed);
//         ASSERT_OK(db->db->erase(itr->first));
//         committed.erase(itr);
//     }
//     ASSERT_OK(db->db->commit_txn(1));
//     ASSERT_EQ(db->db->begin_txn(TxnOptions()), 2);
//
//     auto uncommitted = add_records(*db, 1'000);
//     for (std::size_t i = 0; i < 500; ++i) {
//         const auto itr = begin(uncommitted);
//         ASSERT_OK(db->db->erase(itr->first));
//         uncommitted.erase(itr);
//     }
//     ASSERT_OK(db->reopen());
//
//     expect_contains_records(*db->db, committed);
// }

// class DbRecoveryTests
//     : public EnvTestHarness<PosixEnv>,
//       public testing::Test
//{
// protected:
//     ~DbRecoveryTests() override = default;
// };
//
// TEST_F(DbRecoveryTests, RecoversFirstBatch)
//{
//     std::unique_ptr<Env> clone;
//     std::map<std::string, std::string> snapshot;
//
//     {
//         TestDatabase db(env());
//         snapshot = add_records(db, 1'234, true);
//
//         // Simulate a crash by cloning the database before cleanup has occurred.
//         clone.reset(env().clone());
//     }
//     // Create a new database from the cloned data. This database will need to roll the WAL forward to become
//     // consistent.
//     TestDatabase clone_db(*clone);
//     expect_contains_records(*clone_db.db, snapshot);
// }

// TEST_F(DbRecoveryTests, RecoversNthBatch)
//{
//     std::unique_ptr<Env> clone;
//     std::map<std::string, std::string> snapshot;
//
//     {
//         TestDatabase db(env());
//
//         for (std::size_t i = 0; i < 10; ++i) {
//             ASSERT_EQ(db.db->begin_txn(TxnOptions()), i + 1);
//             for (const auto &[k, v] : add_records(db, 1'234)) {
//                 snapshot[k] = v;
//             }
//             ASSERT_OK(db.db->commit_txn(i + 1));
//         }
//
//         clone.reset(env().clone());
//     }
//     TestDatabase clone_db(*clone);
//     ASSERT_OK(clone_db.db->status());
//     expect_contains_records(*clone_db.db, snapshot);
// }
//
// struct ErrorWrapper {
//     std::string filename;
//     tools::Interceptor::Type type;
//     std::size_t successes = 0;
// };
//
// class DbErrorTests
//     : public testing::TestWithParam<ErrorWrapper>,
//       public EnvTestHarness<tools::TestEnv>
//{
// protected:
//     using Base = EnvTestHarness<tools::TestEnv>;
//
//     DbErrorTests()
//     {
//         db = std::make_unique<TestDatabase>(Base::env());
//
//         EXPECT_EQ(db->db->begin_txn(TxnOptions()), 1);
//         committed = add_records(*db, 10'000);
//         EXPECT_OK(db->db->commit_txn(1));
//     }
//     ~DbErrorTests() override = default;
//
//     auto set_error() -> void
//     {
//         env().add_interceptor(
//             error.filename,
//             tools::Interceptor(
//                 error.type,
//                 [this] {
//                     if (counter++ >= error.successes) {
//                         return special_error();
//                     }
//                     return Status::ok();
//                 }));
//     }
//
//     auto SetUp() -> void override
//     {
//         set_error();
//     }
//
//     ErrorWrapper error{GetParam()};
//     std::unique_ptr<TestDatabase> db;
//     std::map<std::string, std::string> committed;
//     std::size_t counter = 0;
// };
//
// TEST_P(DbErrorTests, HandlesReadErrorDuringQuery)
//{
//     for (std::size_t iteration = 0; iteration < 2; ++iteration) {
//         for (const auto &[k, v] : committed) {
//             std::string value;
//             const auto s = db->db->get(k, &value);
//
//             if (!s.is_ok()) {
//                 assert_special_error(s);
//                 break;
//             }
//         }
//         ASSERT_OK(db->db->status());
//         counter = 0;
//     }
// }
//
// TEST_P(DbErrorTests, HandlesReadErrorDuringIteration)
//{
//     std::unique_ptr<Cursor> cursor(db->db->new_cursor());
//     cursor->seek_first();
//     while (cursor->is_valid()) {
//         (void)cursor->key();
//         (void)cursor->value();
//         cursor->next();
//     }
//     assert_special_error(cursor->status());
//     ASSERT_OK(db->db->status());
//     counter = 0;
//
//     cursor->seek_last();
//     while (cursor->is_valid()) {
//         (void)cursor->key();
//         (void)cursor->value();
//         cursor->previous();
//     }
//     assert_special_error(cursor->status());
//     ASSERT_OK(db->db->status());
// }
//
// TEST_P(DbErrorTests, HandlesReadErrorDuringSeek)
//{
//     std::unique_ptr<Cursor> cursor(db->db->new_cursor());
//
//     for (const auto &[k, v] : committed) {
//         cursor->seek(k);
//         if (!cursor->is_valid()) {
//             break;
//         }
//     }
//     assert_special_error(cursor->status());
//     ASSERT_OK(db->db->status());
// }
//
// INSTANTIATE_TEST_SUITE_P(
//     DbErrorTests,
//     DbErrorTests,
//     ::testing::Values(
//         ErrorWrapper{"./test", tools::Interceptor::kRead, 0},
//         ErrorWrapper{"./test", tools::Interceptor::kRead, 1},
//         ErrorWrapper{"./test", tools::Interceptor::kRead, 10}));
//
// class DbFatalErrorTests : public DbErrorTests
//{
// protected:
//     ~DbFatalErrorTests() override = default;
//
//     auto SetUp() -> void override
//     {
//         const auto txn = db->db->begin_txn(TxnOptions());
//         tools::RandomGenerator random;
//         for (const auto &[k, v] : tools::fill_db(*db->db, random, 1'000, db->kPageSize * 10)) {
//             ASSERT_OK(db->db->erase(k));
//         }
//         ASSERT_OK(db->db->commit_txn(txn));
//         DbErrorTests::SetUp();
//     }
// };
//
// TEST_P(DbFatalErrorTests, ErrorsDuringModificationsAreFatal)
//{
//     while (db->db->status().is_ok()) {
//         const auto txn = db->db->begin_txn(TxnOptions());
//         auto itr = begin(committed);
//         for (std::size_t i = 0; i < committed.size() && db->db->erase((itr++)->first).is_ok(); ++i)
//             ;
//         for (std::size_t i = 0; i < committed.size() && db->db->put((itr++)->first, "value").is_ok(); ++i)
//             ;
//         assert_special_error(db->db->commit_txn(txn));
//     }
//     assert_special_error(db->db->status());
//     assert_special_error(db->db->put("key", "value"));
// }
//
// TEST_P(DbFatalErrorTests, OperationsAreNotPermittedAfterFatalError)
//{
//     const auto txn = db->db->begin_txn(TxnOptions());
//     auto itr = begin(committed);
//     while (db->db->erase(itr++->first).is_ok()) {
//         ASSERT_NE(itr, end(committed));
//     }
//     assert_special_error(db->db->status());
//     assert_special_error(db->db->commit_txn(txn));
//     assert_special_error(db->db->put("key", "value"));
//     std::string value;
//     assert_special_error(db->db->get("key", &value));
//     auto *cursor = db->db->new_cursor();
//     assert_special_error(cursor->status());
//     delete cursor;
// }
//
//// TODO: This doesn't exercise much of what can go wrong here. Need a test for failure to truncate the file, so the
////       header page count is left incorrect. We should be able to recover from that.
// TEST_P(DbFatalErrorTests, RecoversFromVacuumFailure)
//{
//     (void)db->db->begin_txn(TxnOptions());
//     assert_special_error(db->db->vacuum());
//     delete db->db;
//     db->db = nullptr;
//
//     env().clear_interceptors();
//     ASSERT_OK(DB::open(db->options, "./test", db->db));
//     tools::validate_db(*db->db);
//
//     for (const auto &[key, value] : committed) {
//         std::string result;
//         ASSERT_OK(db->db->get(key, &result));
//         ASSERT_EQ(result, value);
//     }
//     tools::validate_db(*db->db);
//
//     std::size_t file_size;
//     ASSERT_OK(env().file_size("./test", file_size));
//     ASSERT_EQ(file_size, db_impl(db->db)->TEST_pager().page_count() * db->kPageSize);
// }
//
// INSTANTIATE_TEST_SUITE_P(
//     DbFatalErrorTests,
//     DbFatalErrorTests,
//     ::testing::Values(
//         ErrorWrapper{"./wal", tools::Interceptor::kRead, 1},
//         ErrorWrapper{"./wal", tools::Interceptor::kRead, 5},
//         ErrorWrapper{"./wal", tools::Interceptor::kWrite, 0},
//         ErrorWrapper{"./wal", tools::Interceptor::kWrite, 1},
//         ErrorWrapper{"./wal", tools::Interceptor::kWrite, 5}));
//
// class DbOpenTests
//     : public EnvTestHarness<PosixEnv>,
//       public testing::Test
//{
// protected:
//     DbOpenTests()
//     {
//         options.env = &env();
//     }
//
//     ~DbOpenTests() override = default;
//
//     Options options;
//     DB *db;
// };
//
// TEST_F(DbOpenTests, CreatesMissingDb)
//{
//     options.error_if_exists = false;
//     options.create_if_missing = true;
//     ASSERT_OK(DB::open(options, kDBFilename, db));
//     delete db;
//
//     options.create_if_missing = false;
//     ASSERT_OK(DB::open(options, kDBFilename, db));
//     delete db;
// }
//
// TEST_F(DbOpenTests, FailsIfMissingDb)
//{
//     options.create_if_missing = false;
//     ASSERT_TRUE(DB::open(options, kDBFilename, db).is_invalid_argument());
// }
//
// TEST_F(DbOpenTests, FailsIfDbExists)
//{
//     options.create_if_missing = true;
//     options.error_if_exists = true;
//     ASSERT_OK(DB::open(options, kDBFilename, db));
//     delete db;
//
//     options.create_if_missing = false;
//     ASSERT_TRUE(DB::open(options, kDBFilename, db).is_invalid_argument());
// }
//
// class ApiTests
//     : public testing::Test,
//       public EnvTestHarness<tools::TestEnv>
//{
// protected:
//     ApiTests()
//     {
//         options.env = &env();
//         options.wal_filename = kWalFilename;
//     }
//
//     ~ApiTests() override
//     {
//         delete db;
//     }
//
//     auto SetUp() -> void override
//     {
//         ApiTests::reopen();
//     }
//
//     virtual auto reopen() -> void
//     {
//         delete db;
//         db = nullptr;
//
//         ASSERT_OK(DB::open(options, "./test", db));
//     }
//
//     Options options;
//     DB *db = nullptr;
// };
//
// TEST_F(ApiTests, OnlyReturnsValidProperties)
//{
//     // Check for existence.
//     ASSERT_TRUE(db->get_property("calicodb.stats", nullptr));
//     ASSERT_TRUE(db->get_property("calicodb.tables", nullptr));
//     ASSERT_FALSE(db->get_property("Calicodb.stats", nullptr));
//     ASSERT_FALSE(db->get_property("calicodb.nonexistent", nullptr));
//
//     std::string stats, tables, scratch;
//     ASSERT_TRUE(db->get_property("calicodb.stats", &stats));
//     ASSERT_TRUE(db->get_property("calicodb.tables", &tables));
//     ASSERT_FALSE(db->get_property("Calicodb.stats", &scratch));
//     ASSERT_FALSE(db->get_property("calicodb.nonexistent", &scratch));
//     ASSERT_FALSE(stats.empty());
//     ASSERT_FALSE(tables.empty());
//     ASSERT_TRUE(scratch.empty());
// }
//
// TEST_F(ApiTests, IsConstCorrect)
//{
//     ASSERT_OK(db->put("key", "value"));
//
//     auto *cursor = db->new_cursor();
//     cursor->seek_first();
//
//     const auto *const_cursor = cursor;
//     ASSERT_TRUE(const_cursor->is_valid());
//     ASSERT_OK(const_cursor->status());
//     ASSERT_EQ(const_cursor->key(), "key");
//     ASSERT_EQ(const_cursor->value(), "value");
//     delete const_cursor;
//
//     const auto *const_db = db;
//     std::string property;
//     ASSERT_TRUE(const_db->get_property("calicodb.stats", &property));
//     ASSERT_OK(const_db->status());
// }
//
// TEST_F(ApiTests, FirstTxnNumberIsNonzero)
//{
//     ASSERT_NE(db->begin_txn(TxnOptions()), 0);
// }
//
// TEST_F(ApiTests, OnlyRecognizesCurrentTransaction)
//{
//     auto txn = db->begin_txn(TxnOptions());
//
//     // Incorrect transaction number.
//     ASSERT_TRUE(db->commit_txn(txn - 1).is_invalid_argument());
//     ASSERT_TRUE(db->commit_txn(txn + 1).is_invalid_argument());
//     ASSERT_TRUE(db->rollback_txn(txn - 1).is_invalid_argument());
//     ASSERT_TRUE(db->rollback_txn(txn + 1).is_invalid_argument());
//
//     ASSERT_OK(db->commit_txn(txn));
//
//     // Transaction has already completed.
//     ASSERT_TRUE(db->commit_txn(txn - 1).is_invalid_argument());
//     ASSERT_TRUE(db->commit_txn(txn).is_invalid_argument());
//     ASSERT_TRUE(db->commit_txn(txn + 1).is_invalid_argument());
//     ASSERT_TRUE(db->rollback_txn(txn - 1).is_invalid_argument());
//     ASSERT_TRUE(db->rollback_txn(txn).is_invalid_argument());
//     ASSERT_TRUE(db->rollback_txn(txn + 1).is_invalid_argument());
// }
//
// TEST_F(ApiTests, CannotModifyReadOnlyTable)
//{
//     TableOptions options{AccessMode::kReadOnly};
//     Table *readonly, *readwrite;
//
//     ASSERT_OK(db->create_table({}, "table", readwrite));
//     ASSERT_OK(db->put(*readwrite, "4", "2"));
//     db->close_table(readwrite);
//
//     ASSERT_OK(db->create_table(options, "table", readonly));
//
//     // Reading is allowed.
//     std::string result;
//     ASSERT_OK(db->get(*readonly, "4", &result));
//
//     // But not modifications (even if they would do nothing).
//     ASSERT_TRUE(db->put(*readonly, "4", "2").is_invalid_argument());
//     ASSERT_TRUE(db->erase(*readonly, "5").is_invalid_argument());
//     db->close_table(readonly);
// }
//
// TEST_F(ApiTests, EmptyKeysAreNotAllowed)
//{
//     ASSERT_TRUE(db->put("", "value").is_invalid_argument());
// }
//
// TEST_F(ApiTests, UncommittedTransactionIsRolledBack)
//{
//     auto txn = db->begin_txn(TxnOptions());
//     ASSERT_OK(db->put("a", "1"));
//     ASSERT_OK(db->put("b", "2"));
//     ASSERT_OK(db->put("c", "3"));
//     ASSERT_OK(db->commit_txn(txn));
//
//     txn = db->begin_txn(TxnOptions());
//     ASSERT_OK(db->put("a", "x"));
//     ASSERT_OK(db->put("b", "y"));
//     ASSERT_OK(db->put("c", "z"));
//
//     reopen();
//
//     std::string value;
//     ASSERT_OK(db->get("a", &value));
//     ASSERT_EQ(value, "1");
//     ASSERT_OK(db->get("b", &value));
//     ASSERT_EQ(value, "2");
//     ASSERT_OK(db->get("c", &value));
//     ASSERT_EQ(value, "3");
// }
//
// TEST_F(ApiTests, EmptyTransactionsAreOk)
//{
//     ASSERT_OK(db->commit_txn(db->begin_txn(TxnOptions())));
// }
//
// TEST_F(ApiTests, KeysCanBeArbitraryBytes)
//{
//     const std::string key_1("\x00\x00", 2);
//     const std::string key_2("\x00\x01", 2);
//     const std::string key_3("\x01\x00", 2);
//
//     ASSERT_EQ(db->begin_txn(TxnOptions()), 1);
//     ASSERT_OK(db->put(key_1, "1"));
//     ASSERT_OK(db->put(key_2, "2"));
//     ASSERT_OK(db->put(key_3, "3"));
//     ASSERT_OK(db->commit_txn(1));
//
//     auto *cursor = db->new_cursor();
//     cursor->seek_first();
//
//     ASSERT_OK(cursor->status());
//     ASSERT_EQ(cursor->key(), key_1);
//     ASSERT_EQ(cursor->value(), "1");
//     cursor->next();
//
//     ASSERT_OK(cursor->status());
//     ASSERT_EQ(cursor->key(), key_2);
//     ASSERT_EQ(cursor->value(), "2");
//     cursor->next();
//
//     ASSERT_OK(cursor->status());
//     ASSERT_EQ(cursor->key(), key_3);
//     ASSERT_EQ(cursor->value(), "3");
//     cursor->next();
//     delete cursor;
// }
//
// TEST_F(ApiTests, HandlesLargeKeys)
//{
//     tools::RandomGenerator random(kPageSize * 100 * 3);
//
//     const auto key_1 = '\x01' + random.Generate(kPageSize * 100).to_string();
//     const auto key_2 = '\x02' + random.Generate(kPageSize * 100).to_string();
//     const auto key_3 = '\x03' + random.Generate(kPageSize * 100).to_string();
//
//     auto txn = db->begin_txn(TxnOptions());
//     ASSERT_OK(db->put(key_1, "1"));
//     ASSERT_OK(db->put(key_2, "2"));
//     ASSERT_OK(db->put(key_3, "3"));
//     ASSERT_OK(db->commit_txn(txn));
//
//     auto *cursor = db->new_cursor();
//     cursor->seek_first();
//
//     ASSERT_OK(cursor->status());
//     ASSERT_EQ(cursor->key(), key_1);
//     ASSERT_EQ(cursor->value(), "1");
//     cursor->next();
//
//     ASSERT_OK(cursor->status());
//     ASSERT_EQ(cursor->key(), key_2);
//     ASSERT_EQ(cursor->value(), "2");
//     cursor->next();
//
//     ASSERT_OK(cursor->status());
//     ASSERT_EQ(cursor->key(), key_3);
//     ASSERT_EQ(cursor->value(), "3");
//     cursor->next();
//     delete cursor;
// }
//
// TEST_F(ApiTests, CheckIfKeyExists)
//{
//     ASSERT_TRUE(db->get("k", nullptr).is_not_found());
//     ASSERT_OK(db->put("k", "v"));
//     ASSERT_OK(db->get("k", nullptr));
// }
//
// class NoLoggingEnv : public EnvWrapper
//{
//     Env *m_env;
//
// public:
//     explicit NoLoggingEnv()
//         : EnvWrapper(*new tools::FakeEnv),
//           m_env(target())
//     {
//     }
//
//     ~NoLoggingEnv() override
//     {
//         delete m_env;
//     }
//
//     [[nodiscard]] auto new_log_file(const std::string &filename, Sink *&out) -> Status override
//     {
//         return Status::not_supported("logging is not supported");
//     }
// };
//
// TEST(DisabledLoggingTests, LoggingIsNotNecessary)
//{
//     Options options;
//     NoLoggingEnv no_logging_env;
//     options.env = &no_logging_env;
//
//     DB *db;
//     ASSERT_OK(DB::open(options, "db", db));
//
//     tools::RandomGenerator random;
//     tools::expect_db_contains(*db, tools::fill_db(*db, random, 100));
//
//     delete db;
// }
//
// class LargePayloadTests : public ApiTests
//{
// public:
//     explicit LargePayloadTests()
//         : random(kPageSize * 500)
//     {
//     }
//
//     [[nodiscard]] auto random_string(std::size_t max_size) const -> std::string
//     {
//         return random.Generate(random.Next(1, max_size)).to_string();
//     }
//
//     auto run_test(std::size_t max_key_size, std::size_t max_value_size)
//     {
//         auto txn = db->begin_txn(TxnOptions());
//         std::unordered_map<std::string, std::string> map;
//         for (std::size_t i = 0; i < 100; ++i) {
//             const auto key = random_string(max_key_size);
//             const auto value = random_string(max_value_size);
//             ASSERT_OK(db->put(key, value));
//         }
//         ASSERT_OK(db->commit_txn(txn));
//
//         txn = db->begin_txn(TxnOptions());
//         for (const auto &[key, value] : map) {
//             std::string result;
//             ASSERT_OK(db->get(key, &result));
//             ASSERT_EQ(result, value);
//             ASSERT_OK(db->erase(key));
//         }
//         ASSERT_OK(db->commit_txn(txn));
//     }
//
//     tools::RandomGenerator random;
// };
//
// TEST_F(LargePayloadTests, LargeKeys)
//{
//     run_test(100 * kPageSize, 100);
// }
//
// TEST_F(LargePayloadTests, LargeValues)
//{
//     run_test(100, 100 * kPageSize);
// }
//
// TEST_F(LargePayloadTests, LargePayloads)
//{
//     run_test(100 * kPageSize, 100 * kPageSize);
// }
//
// class CommitFailureTests : public ApiTests
//{
// protected:
//     ~CommitFailureTests() override = default;
//
//     auto SetUp() -> void override
//     {
//         ApiTests::SetUp();
//
//         tools::RandomGenerator random;
//         ASSERT_EQ(db->begin_txn(TxnOptions()), 1);
//         commits[false] = tools::fill_db(*db, random, 5'000);
//         ASSERT_OK(db->commit_txn(1));
//
//         ASSERT_EQ(db->begin_txn(TxnOptions()), 2);
//         commits[true] = tools::fill_db(*db, random, 5'678);
//         for (const auto &record : commits[false]) {
//             commits[true].insert(record);
//         }
//     }
//
//     auto reopen() -> void override
//     {
//         env().clear_interceptors();
//         ApiTests::reopen();
//     }
//
//     auto run_test(bool persisted) -> void
//     {
//         ASSERT_OK(db->status());
//         const auto s = db->commit_txn(2);
//         ASSERT_EQ(s.is_ok(), persisted);
//         assert_special_error(db->status());
//
//         reopen();
//
//         for (const auto &[key, value] : commits[persisted]) {
//             std::string result;
//             ASSERT_OK(db->get(key, &result));
//             ASSERT_EQ(value, result);
//         }
//     }
//
//     std::map<std::string, std::string> commits[2];
// };
//
// TEST_F(CommitFailureTests, WalFlushFailure)
//{
//     QUICK_INTERCEPTOR(kWalFilename, tools::Interceptor::kWrite);
//     run_test(false);
// }
//
// class WalPrefixTests
//     : public EnvTestHarness<PosixEnv>,
//       public testing::Test
//{
// public:
//     WalPrefixTests()
//     {
//         options.env = &env();
//     }
//
//     Options options;
//     DB *db = nullptr;
// };
//
// TEST_F(WalPrefixTests, WalDirectoryMustExist)
//{
//     options.wal_filename = "./nonexistent/wal";
//     ASSERT_TRUE(DB::open(options, kDBFilename, db).is_not_found());
// }

class DBTests
    : public testing::TestWithParam<ConcurrencyTestParam>,
      public ConcurrencyTestHarness<PosixEnv>
{
protected:
    explicit DBTests()
    {
        m_options.cache_size = kMinFrameCount * kPageSize;
        m_options.env = &env();
    }

    ~DBTests() override = default;

    Options m_options;
};

TEST_P(DBTests, Open)
{
    run_test(GetParam(), [this](auto &, auto, auto) {
        DB *db;
        EXPECT_OK(DB::open(m_options, kDBFilename, db));
        delete db;
        return false;
    });
}
TEST_P(DBTests, StartReading)
{
    run_test(GetParam(), [this](auto &, auto, auto) {
        DB *db;
        EXPECT_OK(DB::open(m_options, kDBFilename, db));

        Txn *txn;
        EXPECT_OK(db->start(false, txn));
        db->finish(txn);

        delete db;
        return false;
    });
}
TEST_P(DBTests, StartWriting)
{
    run_test(GetParam(), [this](auto &, auto, auto t) {
        // Create 1 writer in each process.
        const auto is_writer = t == 0;
        Txn *txn = nullptr;
        DB *db = nullptr;

        EXPECT_OK(DB::open(m_options, kDBFilename, db));

        ScopeGuard guard = [&db, &txn] {
            db->finish(txn);
            delete db;
        };

        if (is_writer) {
            Status s;
            do {
                s = db->start(true, txn);
            } while (s.is_busy());
            EXPECT_OK(s);
        } else {
            EXPECT_OK(db->start(false, txn));
        }

        Table *table;
        auto s = txn->new_table(TableOptions(), "table", table);
        if (s.is_invalid_argument() && !is_writer) {
            // Loop until a writer creates the table.
            return true;
        }
        EXPECT_OK(s);
        std::string buffer;
        s = table->get("key", &buffer);
        EXPECT_TRUE(s.is_ok() || s.is_not_found())
            << get_status_name(s) << ": " << s.to_string();
        Slice value(buffer);

        if (is_writer) {
            if (s.is_ok()) {
                U64 number;
                EXPECT_TRUE(consume_decimal_number(value, &number))
                    << "corrupted read: " << escape_string(value);
                value = number_to_string(number + 1);
            } else {
                value = "1";
            }
            EXPECT_OK(table->put("key", value));
            EXPECT_OK(txn->commit());
        }
        delete table;
        return false;
    });

    DB *db;
    ASSERT_OK(DB::open(m_options, kDBFilename, db));

    Txn *txn;
    ASSERT_OK(db->start(false, txn));

    Table *table;
    ASSERT_OK(txn->new_table(TableOptions(), "table", table));

    std::string buffer;
    ASSERT_OK(table->get("key", &buffer));

    U64 number;
    Slice value(buffer);
    ASSERT_TRUE(consume_decimal_number(value, &number));

    db->finish(txn);
    delete db;

    ASSERT_EQ(GetParam().num_processes, number);
}

INSTANTIATE_TEST_SUITE_P(DBTests_SanityCheck, DBTests, kConcurrencySanityCheckValues);
INSTANTIATE_TEST_SUITE_P(DBTests_MT, DBTests, kMultiThreadConcurrencyValues);
INSTANTIATE_TEST_SUITE_P(DBTests_MP, DBTests, kMultiProcessConcurrencyValues);
INSTANTIATE_TEST_SUITE_P(DBTests_MX, DBTests, kMultiProcessMultiThreadConcurrencyValues);

} // namespace calicodb
