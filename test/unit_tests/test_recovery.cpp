//// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
//// This source code is licensed under the MIT License, which can be found in
//// LICENSE.md. See AUTHORS.md for a list of contributor names.
////
//// Recovery tests (harness is modified from LevelDB).
//
// #include "calicodb/db.h"
// #include "tools.h"
// #include "unit_tests.h"
//
// namespace calicodb
//{
//
// template <class EnvType = tools::TestEnv>
// class RecoveryTestHarness : public EnvTestHarness<EnvType>
//{
// public:
//    using Base = EnvTestHarness<EnvType>;
//    static constexpr auto kPageSize = kMinPageSize;
//
//    RecoveryTestHarness()
//    {
//        db_options.wal_filename = kWalFilename;
//        db_options.page_size = kPageSize;
//        db_options.cache_size = kMinPageSize * 16;
//        db_options.env = &Base::env();
//
//        // TODO: Running these in sync file_lock right now, it's easier to tell how the DB should
//        //       look. Should test not sync file_lock as well. Will likely lose more than 1 transaction,
//        //       but the DB should not become corrupted.
//        db_options.sync = true;
//
//        open();
//    }
//
//    ~RecoveryTestHarness() override
//    {
//        delete db;
//    }
//
//    virtual auto close() -> void
//    {
//        delete db;
//        db = nullptr;
//    }
//
//    auto open_with_status(Options *options = nullptr) -> Status
//    {
//        close();
//        Options opts = db_options;
//        if (options != nullptr) {
//            opts = *options;
//        }
//        if (opts.env == nullptr) {
//            opts.env = &Base::env();
//        }
//        return DB::open(opts, kDBFilename, db);
//    }
//
//    auto open(Options *options = nullptr) -> void
//    {
//        ASSERT_OK(open_with_status(options));
//    }
//
//    auto put(const std::string &k, const std::string &v) const -> Status
//    {
//        return db->put(k, v);
//    }
//
//    auto get(const std::string &k) const -> std::string
//    {
//        std::string result;
//        const auto s = db->get(k, &result);
//        if (s.is_not_found()) {
//            result = "NOT_FOUND";
//        } else if (!s.is_ok()) {
//            result = s.to_string();
//        }
//        return result;
//    }
//
//    [[nodiscard]] auto num_wal_frames() const -> std::size_t
//    {
//        const auto size = file_size(kWalFilename);
//        if (size > 32) {
//            return (size - 32) / (kPageSize + 24);
//        }
//        return 0;
//    }
//
//    [[nodiscard]] auto file_size(const std::string &fname) const -> std::size_t
//    {
//        std::size_t result;
//        EXPECT_OK(Base::env().file_size(fname, result));
//        return result;
//    }
//
//    tools::RandomGenerator random;
//    Options db_options;
//    DB *db = nullptr;
//};
//
// class RecoveryTests
//    : public RecoveryTestHarness<>,
//      public testing::Test
//{
// protected:
//    static constexpr std::size_t kN = 500;
//};
//
// TEST_F(RecoveryTests, NormalShutdown)
//{
//    ASSERT_EQ(num_wal_frames(), 0);
//    // begin_txn() was not called, so there will be 3 implicit transactions,
//    // requiring 3 commit WAL frames to be written.
//    ASSERT_OK(put("a", "1"));
//    ASSERT_OK(put("b", "2"));
//    ASSERT_OK(put("c", "3"));
//    ASSERT_EQ(num_wal_frames(), 3);
//    close();
//
//    ASSERT_FALSE(Base::env().file_exists(kWalFilename));
//}
//
// TEST_F(RecoveryTests, RollbackA)
//{
//    std::string prefix;
//    for (std::size_t i = 0; i < kN; ++i) {
//        auto txn = db->begin_txn(TxnOptions());
//        ASSERT_OK(put(prefix + "a", "1"));
//        ASSERT_OK(put(prefix + "b", "2"));
//        ASSERT_OK(put(prefix + "c", "3"));
//        ASSERT_OK(db->commit_txn(txn));
//
//        txn = db->begin_txn(TxnOptions());
//        ASSERT_OK(put(prefix + "c", "X"));
//        ASSERT_OK(put(prefix + "d", "4"));
//        if (i & 1) {
//            // If rollback_txn() is not called, rollback happens automatically when the DB
//            // is closed (or the next call to DB::open() if an error occurred).
//            ASSERT_OK(db->rollback_txn(txn));
//        }
//        open();
//
//        ASSERT_EQ(get(prefix + "a"), "1");
//        ASSERT_EQ(get(prefix + "b"), "2");
//        ASSERT_EQ(get(prefix + "c"), "3");
//        ASSERT_EQ(get(prefix + "d"), "NOT_FOUND");
//        prefix += '_';
//    }
//}
//
// TEST_F(RecoveryTests, RollbackB)
//{
//    for (std::size_t iteration = 0; iteration < 3; ++iteration) {
//        // Keep these changes.
//        const auto base = iteration * kN;
//        auto txn = db->begin_txn(TxnOptions());
//        for (std::size_t i = 0; i < kN; ++i) {
//            const auto key = tools::integral_key(base + i);
//            ASSERT_OK(put(key, key));
//        }
//        ASSERT_OK(db->commit_txn(txn));
//
//        // Rollback these changes.
//        txn = db->begin_txn(TxnOptions());
//        for (std::size_t i = 0; i < kN; ++i) {
//            ASSERT_OK(db->erase(tools::integral_key(base + i)));
//        }
//        for (std::size_t i = kN; i < kN * 2; ++i) {
//            ASSERT_OK(put(tools::integral_key(base + i), "42"));
//        }
//
//        // Every possible combination these 2 calls should produce the same
//        // outcome: rollback of the current transaction.
//        if (iteration <= 1) {
//            ASSERT_OK(db->rollback_txn(txn));
//        }
//        if (iteration >= 1) {
//            open();
//        }
//
//        // Only the committed changes should persist.
//        for (std::size_t i = 0; i < kN * 2; ++i) {
//            const auto key = tools::integral_key(base + i);
//            ASSERT_EQ(get(key), i < kN ? key : "NOT_FOUND");
//        }
//    }
//}
//
// TEST_F(RecoveryTests, RollbackC)
//{
//    auto records = tools::fill_db(*db, random, kN);
//    open();
//
//    auto txn = db->begin_txn(TxnOptions());
//    tools::fill_db(*db, random, kN);
//    ASSERT_OK(db->rollback_txn(txn));
//
//    for (const auto &[key, value] : records) {
//        ASSERT_EQ(get(key), value);
//    }
//}
//
// TEST_F(RecoveryTests, RollbackD)
//{
//    auto records = tools::fill_db(*db, random, kN);
//    open();
//    const auto actual = tools::read_file_to_string(*m_env, kDBFilename).substr(kPageSize * 2, kPageSize);
//
//    for (std::size_t iteration = 0; iteration < 3; ++iteration) {
//        auto txn = db->begin_txn(TxnOptions());
//        for (std::size_t i = 0; i < kN; ++i) {
//            // Same keys each time. Since what we did before was rolled back, these
//            // keys don't exist anyway.
//            const auto key = tools::integral_key(i);
//            ASSERT_OK(put(key, key));
//        }
//        ASSERT_OK(db->rollback_txn(txn));
//
//        const auto before = tools::read_file_to_string(*m_env, kDBFilename).substr(kPageSize * 2, kPageSize);
//
//        if (iteration & 1) {
//            open();
//        }
//        const auto after = tools::read_file_to_string(*m_env, kDBFilename).substr(kPageSize * 2, kPageSize);
//
//        for (const auto &[key, value] : records) {
//            ASSERT_EQ(get(key), value);
//        }
//    }
//}
//
// TEST_F(RecoveryTests, VacuumRecovery)
//{
//    auto txn = db->begin_txn(TxnOptions());
//    const auto committed = tools::fill_db(*db, random, 5'000);
//    ASSERT_OK(db->commit_txn(txn));
//    txn = db->begin_txn(TxnOptions());
//
//    for (std::size_t i = 0; i < 1'000; ++i) {
//        ASSERT_OK(db->put(tools::integral_key(i), random.Generate(db_options.page_size)));
//    }
//    for (std::size_t i = 0; i < 1'000; ++i) {
//        ASSERT_OK(db->erase(tools::integral_key(i)));
//    }
//
//    // Grow the database, then make freelist pages.
//    for (std::size_t i = 0; i < 1'000; ++i) {
//        ASSERT_OK(db->put(tools::integral_key(i), random.Generate(db_options.page_size)));
//    }
//    for (std::size_t i = 0; i < 1'000; ++i) {
//        ASSERT_OK(db->erase(tools::integral_key(i)));
//    }
//    // Shrink the database.
//    ASSERT_OK(db->vacuum());
//
//    // Grow the database again. This time, it will look like we need to write image records
//    // for the new pages, even though they are already in the WAL.
//    for (std::size_t i = 0; i < 1'000; ++i) {
//        ASSERT_OK(db->put(tools::integral_key(i), random.Generate(db_options.page_size)));
//    }
//
//    // Now reopen the database and roll the WAL.
//    open();
//
//    std::string result;
//    for (const auto &[key, value] : committed) {
//        ASSERT_OK(db->get(key, &result));
//        ASSERT_EQ(result, value);
//    }
//    db_impl(db)->TEST_validate();
//}
//
// TEST_F(RecoveryTests, SanityCheck)
//{
//    std::map<std::string, std::string> map;
//
//    for (std::size_t i = 0; i < kN; ++i) {
//        const auto k = random.Generate(db_options.page_size * 2);
//        const auto v = random.Generate(db_options.page_size * 4);
//        map[k.to_string()] = v.to_string();
//    }
//
//    unsigned txn;
//    for (std::size_t commit = 0; commit < map.size(); ++commit) {
//        open();
//        txn = db->begin_txn(TxnOptions());
//
//        auto record = begin(map);
//        for (std::size_t index = 0; record != end(map); ++index, ++record) {
//            if (index == commit) {
//                ASSERT_OK(db->commit_txn(txn));
//                txn = db->begin_txn(TxnOptions());
//            } else {
//                ASSERT_OK(db->put(record->first, record->second));
//            }
//        }
//        open();
//
//        record = begin(map);
//        for (std::size_t index = 0; record != end(map); ++index, ++record) {
//            std::string value;
//            if (index < commit) {
//                ASSERT_OK(db->get(record->first, &value));
//                ASSERT_EQ(value, record->second);
//            } else {
//                ASSERT_TRUE(db->get(record->first, &value).is_not_found());
//            }
//        }
//        close();
//
//        ASSERT_OK(DB::free(db_options, kDBFilename));
//    }
//}
//
// class RecoverySanityCheck
//    : public RecoveryTestHarness<>,
//      public testing::TestWithParam<std::tuple<std::string, tools::Interceptor::Type, int>>
//{
// public:
//    explicit RecoverySanityCheck()
//        : interceptor_prefix(std::get<0>(GetParam()))
//    {
//        open();
//
//        tools::RandomGenerator random(1'024 * 1'024 * 8);
//        const std::size_t N = 10'000;
//
//        for (std::size_t i = 0; i < N; ++i) {
//            const auto k = random.Generate(db_options.page_size * 2);
//            const auto v = random.Generate(db_options.page_size * 4);
//            map[k.to_string()] = v.to_string();
//        }
//    }
//
//    ~RecoverySanityCheck() override = default;
//
//    auto SetUp() -> void override
//    {
//        m_txn = db->begin_txn(TxnOptions());
//        auto record = begin(map);
//        for (std::size_t index = 0; record != end(map); ++index, ++record) {
//            ASSERT_OK(db->put(record->first, record->second));
//            if (record->first.front() % 10 == 1) {
//                ASSERT_OK(db->commit_txn(m_txn));
//                m_txn = db->begin_txn(TxnOptions());
//            }
//        }
//        ASSERT_OK(db->commit_txn(m_txn));
//        m_txn = db->begin_txn(TxnOptions());
//
//        COUNTING_INTERCEPTOR(interceptor_prefix, interceptor_type, interceptor_count);
//    }
//
//    auto validate() -> void
//    {
//        CLEAR_INTERCEPTORS();
//        open();
//
//        for (const auto &[k, v] : map) {
//            std::string value;
//            ASSERT_OK(db->get(k, &value));
//            ASSERT_EQ(value, v);
//        }
//    }
//
//    std::string interceptor_prefix;
//    tools::Interceptor::Type interceptor_type = std::get<1>(GetParam());
//    int interceptor_count = std::get<2>(GetParam());
//    std::map<std::string, std::string> map;
//    unsigned m_txn = 0;
//};
//
// TEST_P(RecoverySanityCheck, FailureWhileRunning)
//{
//    for (const auto &[k, v] : map) {
//        auto s = db->erase(k);
//        if (!s.is_ok()) {
//            assert_special_error(s);
//            break;
//        }
//    }
//    if (db->status().is_ok()) {
//        (void)db->vacuum();
//    }
//    assert_special_error(db->status());
//
//    validate();
//}
//
//// TODO: Find some way to determine if an error occurred during the destructor. It happens in each
////       instance except for when we attempt to fail due to a WAL write error, since the WAL is not
////       written during the erase/recovery routine.
// TEST_P(RecoverySanityCheck, FailureDuringClose)
//{
//     // The final transaction committed successfully, so the data we added should persist.
//     close();
//
//     validate();
// }
//
// TEST_P(RecoverySanityCheck, FailureDuringCloseWithUncommittedUpdates)
//{
//     while (db->status().is_ok()) {
//         (void)db->put(random.Generate(16), random.Generate(100));
//     }
//
//     close();
//     validate();
// }
//
// INSTANTIATE_TEST_SUITE_P(
//     RecoverySanityCheck,
//     RecoverySanityCheck,
//     ::testing::Values(
//         std::make_tuple(kDBFilename, tools::Interceptor::kRead, 0),
//         std::make_tuple(kDBFilename, tools::Interceptor::kRead, 1),
//         std::make_tuple(kDBFilename, tools::Interceptor::kRead, 5),
//         std::make_tuple(kWalFilename, tools::Interceptor::kRead, 0),
//         std::make_tuple(kWalFilename, tools::Interceptor::kRead, 1),
//         std::make_tuple(kWalFilename, tools::Interceptor::kRead, 5),
//         std::make_tuple(kWalFilename, tools::Interceptor::kWrite, 0),
//         std::make_tuple(kWalFilename, tools::Interceptor::kWrite, 1),
//         std::make_tuple(kWalFilename, tools::Interceptor::kWrite, 5)));
//
// class OpenErrorTests : public RecoverySanityCheck
//{
// public:
//     ~OpenErrorTests() override = default;
//
//     auto SetUp() -> void override
//     {
//         RecoverySanityCheck::SetUp();
//         const auto saved_count = interceptor_count;
//         interceptor_count = 0;
//
//         // Should fail on the first syscall given by "std::get<1>(GetParam())".
//         close();
//
//         interceptor_count = saved_count;
//     }
// };
//
// TEST_P(OpenErrorTests, FailureDuringOpen)
//{
//     assert_special_error(open_with_status());
//     validate();
// }
//
// INSTANTIATE_TEST_SUITE_P(
//     OpenErrorTests,
//     OpenErrorTests,
//     ::testing::Values(
//         std::make_tuple(kDBFilename, tools::Interceptor::kRead, 0),
//         std::make_tuple(kDBFilename, tools::Interceptor::kRead, 1)));
//
// class DataLossTests
//     : public RecoveryTestHarness<tools::TestEnv>,
//       public testing::TestWithParam<std::size_t>
//{
// public:
//     using Base = RecoveryTestHarness<tools::TestEnv>;
//     const std::size_t kCommitInterval = GetParam();
//
//     ~DataLossTests() override = default;
//
//     auto close() -> void override
//     {
//         // Hack to force an error to occur. The DB won't attempt to recover on close()
//         // in this case. It will have to wait until open().
//         //        const_cast<DBState &>(db_impl(db)->TEST_state()).status = special_error();
//
//         RecoveryTestHarness::close();
//
//         drop_unsynced_wal_data();
//         drop_unsynced_db_data();
//     }
//
//     auto drop_unsynced_wal_data() -> void
//     {
//         Base::env().drop_after_last_sync(kWalFilename);
//     }
//
//     auto drop_unsynced_db_data() -> void
//     {
//         Base::env().drop_after_last_sync(kDBFilename);
//     }
// };
//
// TEST_P(DataLossTests, LossBeforeFirstCheckpoint)
//{
//     for (std::size_t i = 0; i < kCommitInterval; ++i) {
//         ASSERT_OK(db->put(tools::integral_key(i), "value"));
//     }
//     open();
// }
//
// TEST_P(DataLossTests, RecoversLastCheckpoint)
//{
//     auto txn = db->begin_txn(TxnOptions());
//     for (std::size_t i = 0; i < kCommitInterval * 10; ++i) {
//         if (i && i % kCommitInterval == 0) {
//             // All updates are committed except for the last kCommitInterval writes.
//             ASSERT_OK(db->commit_txn(txn));
//             txn = db->begin_txn(TxnOptions());
//         }
//         ASSERT_OK(db->put(tools::integral_key(i), tools::integral_key(i)));
//     }
//     open();
//
//     for (std::size_t i = 0; i < kCommitInterval * 10; ++i) {
//         std::string value;
//         const auto s = db->get(tools::integral_key(i), &value);
//         if (i < kCommitInterval * 9) {
//             ASSERT_OK(s);
//             ASSERT_EQ(value, tools::integral_key(i));
//         } else {
//             ASSERT_TRUE(s.is_not_found());
//         }
//     }
// }
//
// TEST_P(DataLossTests, LongTransaction)
//{
//     auto txn = db->begin_txn(TxnOptions());
//     for (std::size_t i = 0; i < kCommitInterval * 10; ++i) {
//         ASSERT_OK(db->put(tools::integral_key(i), tools::integral_key(i)));
//         if (i % kCommitInterval == kCommitInterval - 1) {
//             ASSERT_OK(db->commit_txn(txn));
//             txn = db->begin_txn(TxnOptions());
//         }
//     }
//
//     for (std::size_t i = 0; i < kCommitInterval * 10; ++i) {
//         ASSERT_OK(db->erase(tools::integral_key(i)));
//     }
//     ASSERT_OK(db->vacuum());
//
//     open();
//
//     for (std::size_t i = 0; i < kCommitInterval * 10; ++i) {
//         std::string value;
//         ASSERT_OK(db->get(tools::integral_key(i), &value));
//         ASSERT_EQ(value, tools::integral_key(i));
//     }
// }
//
// INSTANTIATE_TEST_SUITE_P(
//     DataLossTests,
//     DataLossTests,
//     ::testing::Values(1, 10, 100, 1'000, 10'000));
//
// } // namespace calicodb
