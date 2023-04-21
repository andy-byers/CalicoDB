//// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
//// This source code is licensed under the MIT License, which can be found in
//// LICENSE.md. See AUTHORS.md for a list of contributor names.
//
// #include "db_impl.h"
// #include "header.h"
// #include "logging.h"
// #include "tools.h"
// #include "tree.h"
// #include "unit_tests.h"
// #include "wal.h"
// #include <array>
// #include <filesystem>
// #include <gtest/gtest.h>
// #include <vector>
//
// namespace calicodb
//{
//
// namespace fs = std::filesystem;
//
// TEST(LeakTests, DestroysOwnObjects)
//{
//    fs::remove_all("__calicodb_test");
//
//    DB *db;
//    Table *table;
//
//    ASSERT_OK(DB::open({}, "__calicodb_test", db));
//    ASSERT_OK(db->create_table({}, "test", table));
//    auto *cursor = db->new_cursor(*table);
//
//    delete cursor;
//    db->close_table(table);
//    delete db;
//
//    ASSERT_OK(DB::destroy({}, "__calicodb_test"));
//}
//
// TEST(LeakTests, LeavesUserObjects)
//{
//    Options options;
//    options.env = new tools::FakeEnv;
//    options.info_log = nullptr; // TODO
//
//    DB *db;
//    ASSERT_OK(DB::open(options, "__calicodb_test", db));
//    delete db;
//
//    delete options.info_log;
//    delete options.env;
//}
//
// TEST(BasicDestructionTests, OnlyDeletesCalicoDatabases)
//{
//    std::filesystem::remove_all("./testdb");
//
//    Options options;
//    options.env = Env::default_env();
//
//    // "./test" does not exist.
//    ASSERT_TRUE(DB::destroy(options, "./testdb").is_invalid_argument());
//    ASSERT_FALSE(options.env->file_exists("./testdb"));
//
//    // File is too small to read the header.
//    File *file;
//    ASSERT_OK(options.env->new_file("./testdb", Env::kCreate | Env::kReadWrite, file));
//    ASSERT_TRUE(DB::destroy(options, "./testdb").is_invalid_argument());
//    ASSERT_TRUE(options.env->file_exists("./testdb"));
//
//    // Identifier is incorrect.
//    char buffer[FileHeader::kSize];
//    FileHeader header;
//    header.write(buffer);
//    ++buffer[0];
//    ASSERT_OK(file->write(0, Slice(buffer, sizeof(buffer))));
//    ASSERT_TRUE(DB::destroy(options, "./testdb").is_invalid_argument());
//
//    DB *db;
//    std::filesystem::remove_all("./testdb");
//    ASSERT_OK(DB::open(options, "./testdb", db));
//    ASSERT_OK(DB::destroy(options, "./testdb"));
//
//    delete db;
//    delete file;
//    delete options.env;
//}
//
// TEST(BasicDestructionTests, OnlyDeletesCalicoWals)
//{
//    Options options;
//    options.env = new tools::FakeEnv;
//    options.wal_filename = "./wal";
//
//    DB *db;
//    ASSERT_OK(DB::open(options, "./test", db));
//    delete db;
//
//    // These files are not part of the DB.
//    File *editor;
//    ASSERT_OK(options.env->new_file("./wal_", Env::kCreate | Env::kReadWrite, editor));
//    delete editor;
//    ASSERT_OK(options.env->new_file("./test.db", Env::kCreate | Env::kReadWrite, editor));
//    delete editor;
//
//    ASSERT_OK(DB::destroy(options, "./test"));
//    ASSERT_TRUE(options.env->file_exists("./wal_"));
//    ASSERT_TRUE(options.env->file_exists("./test.db"));
//
//    delete options.env;
//}
//
// class BasicDatabaseTests
//    : public EnvTestHarness<tools::FakeEnv>,
//      public testing::Test
//{
// public:
//    BasicDatabaseTests()
//    {
//        options.page_size = kMinPageSize;
//        options.cache_size = options.page_size * frame_count;
//        options.env = &env();
//    }
//
//    ~BasicDatabaseTests() override
//    {
//        delete options.info_log;
//    }
//
//    [[nodiscard]] auto db_page_count() -> std::size_t
//    {
//        std::size_t file_size;
//        EXPECT_OK(m_env->file_size(kDBFilename, file_size));
//        const auto num_pages = file_size / options.page_size;
//        EXPECT_EQ(file_size, num_pages * options.page_size)
//            << "file size is not a multiple of the page size";
//        return num_pages;
//    }
//
//    std::size_t frame_count = 64;
//    Options options;
//};
//
// TEST_F(BasicDatabaseTests, OpensAndCloses)
//{
//    DB *db;
//    for (std::size_t i = 0; i < 3; ++i) {
//        ASSERT_OK(DB::open(options, kDBFilename, db));
//        delete db;
//    }
//    ASSERT_TRUE(env().file_exists(kDBFilename));
//}
//
// TEST_F(BasicDatabaseTests, InitialState)
//{
//    DB *db;
//    ASSERT_OK(DB::open(options, kDBFilename, db));
//    delete db;
//
//    const auto file = tools::read_file_to_string(env(), kDBFilename);
//    ASSERT_EQ(file.size(), options.page_size * 3)
//        << "DB should have allocated 3 pages (root and default table roots on pages "
//           "1 and 3, respectively, and the first pointer map page on page 2)";
//
//    FileHeader header;
//    ASSERT_TRUE(header.read(file.data()))
//        << "version identifier mismatch";
//    ASSERT_EQ(header.page_count, 3);
//    ASSERT_EQ(header.page_size, options.page_size);
//    ASSERT_EQ(header.freelist_head, 0);
//}
//
// TEST_F(BasicDatabaseTests, IsDestroyed)
//{
//    DB *db;
//    ASSERT_OK(DB::open(options, kDBFilename, db));
//    delete db;
//
//    ASSERT_TRUE(env().file_exists(kDBFilename));
//    ASSERT_OK(DB::destroy(options, kDBFilename));
//    ASSERT_FALSE(env().file_exists(kDBFilename));
//}
//
// TEST_F(BasicDatabaseTests, ClampsBadOptionValues)
//{
//    const auto open_and_check = [this] {
//        DB *db;
//        ASSERT_OK(DB::open(options, kDBFilename, db));
//        ASSERT_OK(db->status());
//        delete db;
//        ASSERT_OK(DB::destroy(options, kDBFilename));
//    };
//
//    options.page_size = kMinPageSize / 2;
//    open_and_check();
//    options.page_size = kMaxPageSize * 2;
//    open_and_check();
//    options.page_size = kMinPageSize + 1;
//    open_and_check();
//
//    options.page_size = kMinPageSize;
//    options.cache_size = options.page_size;
//    open_and_check();
//    options.cache_size = 1 << 31;
//    open_and_check();
//}
//
//// CAUTION: PRNG state does not persist between calls.
// static auto insert_random_groups(DB &db, std::size_t num_groups, std::size_t group_size)
//{
//     tools::RandomGenerator random;
//     for (std::size_t iteration = 0; iteration < num_groups; ++iteration) {
//         ASSERT_OK(db.status());
//         const auto txn = db.begin_txn(TxnOptions());
//         tools::fill_db(db, random, group_size);
//         ASSERT_OK(db.commit_txn(txn));
//     }
//     dynamic_cast<const DBImpl &>(db).TEST_validate();
// }
//
// TEST_F(BasicDatabaseTests, InsertOneGroup)
//{
//     DB *db;
//     ASSERT_OK(DB::open(options, kDBFilename, db));
//     insert_random_groups(*db, 1, 500);
//     delete db;
// }
//
// TEST_F(BasicDatabaseTests, InsertMultipleGroups)
//{
//     DB *db;
//     ASSERT_OK(DB::open(options, kDBFilename, db));
//     insert_random_groups(*db, 5, 500);
//     delete db;
// }
//
// TEST_F(BasicDatabaseTests, DataPersists)
//{
//     static constexpr std::size_t kNumIterations = 5;
//     static constexpr std::size_t kGroupSize = 10;
//
//     auto s = Status::ok();
//     tools::RandomGenerator random(4 * 1'024 * 1'024);
//
//     std::map<std::string, std::string> records;
//     DB *db;
//
//     for (std::size_t iteration = 0; iteration < kNumIterations; ++iteration) {
//         ASSERT_OK(DB::open(options, kDBFilename, db));
//         ASSERT_OK(db->status());
//
//         const auto txn = db->begin_txn(TxnOptions());
//         for (const auto &[k, v] : tools::fill_db(*db, random, kGroupSize)) {
//             records.insert_or_assign(k, v);
//         }
//         ASSERT_OK(db->commit_txn(txn));
//         delete db;
//     }
//
//     ASSERT_OK(DB::open(options, kDBFilename, db));
//     for (const auto &[key, value] : records) {
//         std::string value_out;
//         ASSERT_OK(db->get(key, &value_out));
//         ASSERT_EQ(value_out, value);
//     }
//     delete db;
// }
//
// TEST_F(BasicDatabaseTests, HandlesMaximumPageSize)
//{
//     DB *db;
//     tools::RandomGenerator random;
//     options.page_size = kMaxPageSize;
//     ASSERT_OK(DB::open(options, kDBFilename, db));
//     const auto records = tools::fill_db(*db, random, 1);
//     delete db;
//
//     ASSERT_OK(DB::open(options, kDBFilename, db));
//     tools::expect_db_contains(*db, records);
//     delete db;
// }
//
// TEST_F(BasicDatabaseTests, VacuumShrinksDBFileOnCheckpoint)
//{
//     DB *db;
//     ASSERT_OK(DB::open(options, kDBFilename, db));
//     ASSERT_EQ(db_page_count(), 3);
//
//     tools::RandomGenerator random;
//     auto txn = db->begin_txn(TxnOptions());
//     const auto records = tools::fill_db(*db, random, 1'000);
//     ASSERT_OK(db->commit_txn(txn));
//
//     delete db;
//     db = nullptr;
//
//     const auto saved_page_count = db_page_count();
//     ASSERT_GT(saved_page_count, 3)
//         << "DB file was not written during checkpoint";
//
//     ASSERT_OK(DB::open(options, kDBFilename, db));
//     txn = db->begin_txn(TxnOptions());
//     for (const auto &[key, value] : records) {
//         ASSERT_OK(db->erase(key));
//     }
//     ASSERT_OK(db->vacuum());
//     ASSERT_OK(db->commit_txn(txn));
//
//     ASSERT_EQ(saved_page_count, db_page_count())
//         << "file should not be modified until checkpoint";
//
//     delete db;
//
//     ASSERT_EQ(db_page_count(), 3)
//         << "file was not truncated";
// }
//
// class DbVacuumTests
//     : public EnvTestHarness<tools::FakeEnv>,
//       public testing::TestWithParam<std::tuple<std::size_t, std::size_t, bool>>
//{
// public:
//     DbVacuumTests()
//         : random(1'024 * 1'024 * 8),
//           lower_bounds(std::get<0>(GetParam())),
//           upper_bounds(std::get<1>(GetParam())),
//           reopen(std::get<2>(GetParam()))
//     {
//         CALICODB_EXPECT_LE(lower_bounds, upper_bounds);
//         options.page_size = 0x200;
//         options.cache_size = 0x200 * 16;
//         options.env = &env();
//     }
//
//     std::unordered_map<std::string, std::string> map;
//     tools::RandomGenerator random;
//     DB *db = nullptr;
//     Options options;
//     std::size_t lower_bounds = 0;
//     std::size_t upper_bounds = 0;
//     bool reopen = false;
// };
//
// TEST_P(DbVacuumTests, SanityCheck)
//{
//     ASSERT_OK(DB::open(options, kDBFilename, db));
//
//     for (std::size_t iteration = 0; iteration < 4; ++iteration) {
//         if (reopen) {
//             delete db;
//             ASSERT_OK(DB::open(options, kDBFilename, db));
//         }
//         auto txn = db->begin_txn(TxnOptions());
//
//         for (std::size_t batch = 0; batch < 4; ++batch) {
//             while (map.size() < upper_bounds) {
//                 const auto key = random.Generate(10);
//                 const auto value = random.Generate(options.page_size * 2);
//                 ASSERT_OK(db->put(key, value));
//                 map[key.to_string()] = value.to_string();
//             }
//             while (map.size() > lower_bounds) {
//                 const auto key = begin(map)->first;
//                 map.erase(key);
//                 ASSERT_OK(db->erase(key));
//             }
//             ASSERT_OK(db->vacuum());
//             db_impl(db)->TEST_validate();
//         }
//
//         ASSERT_OK(db->commit_txn(txn));
//
//         std::size_t i = 0;
//         for (const auto &[key, value] : map) {
//             ++i;
//             std::string result;
//             ASSERT_OK(db->get(key, &result));
//             ASSERT_EQ(result, value);
//         }
//     }
//     delete db;
// }
//
// INSTANTIATE_TEST_SUITE_P(
//     DbVacuumTests,
//     DbVacuumTests,
//     ::testing::Values(
//         std::make_tuple(0, 50, false),
//         std::make_tuple(0, 50, true),
//         std::make_tuple(10, 50, false),
//         std::make_tuple(10, 50, true),
//         std::make_tuple(0, 2'000, false),
//         std::make_tuple(0, 2'000, true),
//         std::make_tuple(400, 2'000, false),
//         std::make_tuple(400, 2'000, true)));
//
// class TestDatabase
//{
// public:
//     explicit TestDatabase(Env &env)
//     {
//         options.wal_filename = "./wal";
//         options.page_size = kMinPageSize;
//         options.cache_size = 32 * options.page_size;
//         options.env = &env;
//
//         EXPECT_OK(reopen());
//     }
//
//     virtual ~TestDatabase()
//     {
//         delete db;
//     }
//
//     [[nodiscard]] auto reopen() -> Status
//     {
//         delete db;
//
//         db = nullptr;
//
//         return DB::open(options, "./test", db);
//     }
//
//     Options options;
//     tools::RandomGenerator random;
//     DB *db = nullptr;
// };
//
// class DbRevertTests
//     : public EnvTestHarness<tools::FakeEnv>,
//       public testing::Test
//{
// protected:
//     DbRevertTests()
//     {
//         db = std::make_unique<TestDatabase>(env());
//     }
//     ~DbRevertTests() override = default;
//
//     std::unique_ptr<TestDatabase> db;
// };
//
// static auto add_records(TestDatabase &test, std::size_t n)
//{
//     std::map<std::string, std::string> records;
//
//     for (std::size_t i = 0; i < n; ++i) {
//         const auto key_size = test.random.Next(1, test.options.page_size * 2);
//         const auto value_size = test.random.Next(test.options.page_size * 2);
//         const auto key = test.random.Generate(key_size).to_string();
//         const auto value = test.random.Generate(value_size).to_string();
//         EXPECT_OK(test.db->put(key, value));
//         records[key] = value;
//     }
//     return records;
// }
//
// static auto expect_contains_records(const DB &db, const std::map<std::string, std::string> &committed)
//{
//     for (const auto &[key, value] : committed) {
//         std::string result;
//         CHECK_OK(db.get(key, &result));
//         CHECK_EQ(result, value);
//     }
// }
//
// static auto run_revert_test(TestDatabase &db)
//{
//     auto txn = db.db->begin_txn(TxnOptions());
//     const auto committed = add_records(db, 1'000);
//     ASSERT_OK(db.db->commit_txn(txn));
//
//     txn = db.db->begin_txn(TxnOptions());
//     add_records(db, 1'000);
//     // Explicit BEGIN but no COMMIT.
//     ASSERT_OK(db.reopen());
//
//     expect_contains_records(*db.db, committed);
// }
//
// TEST_F(DbRevertTests, RevertsUncommittedBatch_1)
//{
//     run_revert_test(*db);
// }
//
// TEST_F(DbRevertTests, RevertsUncommittedBatch_2)
//{
//     ASSERT_EQ(db->db->begin_txn(TxnOptions()), 1);
//     add_records(*db, 1'000);
//     ASSERT_OK(db->db->commit_txn(1));
//     run_revert_test(*db);
// }
//
// TEST_F(DbRevertTests, RevertsUncommittedBatch_3)
//{
//     run_revert_test(*db);
//     add_records(*db, 1'000);
// }
//
// TEST_F(DbRevertTests, RevertsUncommittedBatch_4)
//{
//     ASSERT_EQ(db->db->begin_txn(TxnOptions()), 1);
//     add_records(*db, 1'000);
//     ASSERT_OK(db->db->commit_txn(1));
//     run_revert_test(*db);
//     add_records(*db, 1'000);
// }
//
// TEST_F(DbRevertTests, RevertsUncommittedBatch_5)
//{
//     for (std::size_t i = 0; i < 100; ++i) {
//         ASSERT_EQ(db->db->begin_txn(TxnOptions()), i + 1);
//         add_records(*db, 100);
//         ASSERT_OK(db->db->commit_txn(i + 1));
//     }
//     run_revert_test(*db);
//     for (std::size_t i = 0; i < 100; ++i) {
//         add_records(*db, 100);
//     }
// }
//
// TEST_F(DbRevertTests, RevertsVacuum_1)
//{
//     ASSERT_EQ(db->db->begin_txn(TxnOptions()), 1);
//     const auto committed = add_records(*db, 1'000);
//     ASSERT_OK(db->db->commit_txn(1));
//
//     ASSERT_EQ(db->db->begin_txn(TxnOptions()), 2);
//     auto uncommitted = add_records(*db, 1'000);
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
//
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
//
// class DbRecoveryTests
//     : public EnvTestHarness<tools::FakeEnv>,
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
//         ASSERT_EQ(db.db->begin_txn(TxnOptions()), 1);
//         snapshot = add_records(db, 1'234);
//         ASSERT_OK(db.db->commit_txn(1));
//
//         // Simulate a crash by cloning the database before cleanup has occurred.
//         clone.reset(env().clone());
//     }
//     // Create a new database from the cloned data. This database will need to roll the WAL forward to become
//     // consistent.
//     TestDatabase clone_db(*clone);
//     ASSERT_OK(clone_db.db->status());
//     expect_contains_records(*clone_db.db, snapshot);
// }
//
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
//         for (const auto &[k, v] : tools::fill_db(*db->db, random, 1'000, db->options.page_size * 10)) {
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
//     ASSERT_EQ(file_size, db_impl(db->db)->TEST_pager().page_count() * db->options.page_size);
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
//     tools::RandomGenerator random(options.page_size * 100 * 3);
//
//     const auto key_1 = '\x01' + random.Generate(options.page_size * 100).to_string();
//     const auto key_2 = '\x02' + random.Generate(options.page_size * 100).to_string();
//     const auto key_3 = '\x03' + random.Generate(options.page_size * 100).to_string();
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
//         : random(options.page_size * 500)
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
//     run_test(100 * options.page_size, 100);
// }
//
// TEST_F(LargePayloadTests, LargeValues)
//{
//     run_test(100, 100 * options.page_size);
// }
//
// TEST_F(LargePayloadTests, LargePayloads)
//{
//     run_test(100 * options.page_size, 100 * options.page_size);
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
//
// } // namespace calicodb
