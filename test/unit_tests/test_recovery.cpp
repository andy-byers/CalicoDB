// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.
//
// Recovery tests (harness is modified from LevelDB).

#include "calicodb/db.h"
#include "tools.h"
#include "unit_tests.h"

namespace calicodb
{

template <class EnvType = tools::FaultInjectionEnv>
class RecoveryTestHarness
{
public:
    static constexpr auto kPageSize = kMinPageSize;
    static constexpr auto kFilename = "./test";
    static constexpr auto kWalFilename = "./wal";

    RecoveryTestHarness()
        : db_prefix {kFilename}
    {
        env = std::make_unique<EnvType>();
        db_options.wal_filename = kWalFilename;
        db_options.page_size = kPageSize;
        db_options.cache_size = kMinPageSize * 16;
        db_options.env = env.get();

        // TODO: Running these in sync mode right now, it's easier to tell how the DB should
        //       look. Should test not sync mode as well. Will likely lose more than 1 transaction,
        //       but the DB should not become corrupted.
        db_options.sync = true;

        open();
    }

    virtual ~RecoveryTestHarness()
    {
        delete db;
    }

    virtual auto close() -> void
    {
        delete db;
        db = nullptr;
    }

    auto open_with_status(Options *options = nullptr) -> Status
    {
        close();
        Options opts = db_options;
        if (options != nullptr) {
            opts = *options;
        }
        if (opts.env == nullptr) {
            opts.env = env.get();
        }
        return DB::open(opts, db_prefix, db);
    }

    auto open(Options *options = nullptr) -> void
    {
        ASSERT_OK(open_with_status(options));
    }

    auto put(const std::string &k, const std::string &v) const -> Status
    {
        return db->put(k, v);
    }

    auto get(const std::string &k) const -> std::string
    {
        std::string result;
        const auto s = db->get(k, &result);
        if (s.is_not_found()) {
            result = "NOT_FOUND";
        } else if (!s.is_ok()) {
            result = s.to_string();
        }
        return result;
    }

    [[nodiscard]] auto num_wal_frames() const -> std::size_t
    {
        const auto size = file_size(kWalFilename);
        if (size > 32) {
            return (size - 32) / (kPageSize + 24);
        }
        return 0;
    }

    [[nodiscard]] auto file_size(const std::string &fname) const -> std::size_t
    {
        std::size_t result;
        EXPECT_OK(env->file_size(fname, result));
        return result;
    }

    tools::RandomGenerator random {1024 * 1024 * 4};
    std::unique_ptr<EnvType> env;
    Options db_options;
    std::string db_prefix;
    DB *db = nullptr;
};

class RecoveryTests
    : public RecoveryTestHarness<>,
      public testing::Test
{
};

TEST_F(RecoveryTests, NormalShutdown)
{
    ASSERT_EQ(num_wal_frames(), 0);
    ASSERT_EQ(db->begin_txn(TxnOptions()), 1);
    ASSERT_OK(put("a", "1"));
    ASSERT_OK(put("b", "2"));
    ASSERT_OK(put("c", "3"));
    ASSERT_OK(db->commit_txn(1));
    ASSERT_EQ(num_wal_frames(), 1);
    close();

    ASSERT_FALSE(env->file_exists(kWalFilename));
}

TEST_F(RecoveryTests, OnlyCommittedUpdatesArePersisted)
{
    ASSERT_EQ(db->begin_txn(TxnOptions()), 1);
    ASSERT_OK(put("a", "1"));
    ASSERT_OK(put("b", "2"));
    ASSERT_OK(put("c", "3"));
    ASSERT_OK(db->commit_txn(1));

    ASSERT_EQ(db->begin_txn(TxnOptions()), 1);
    ASSERT_OK(put("c", "X"));
    ASSERT_OK(put("d", "4"));
    open();

    ASSERT_EQ(get("a"), "1");
    ASSERT_EQ(get("b"), "2");
    ASSERT_EQ(get("c"), "3");
    ASSERT_EQ(get("d"), "NOT_FOUND");
}

TEST_F(RecoveryTests, RevertsNthTransaction)
{
    ASSERT_EQ(db->begin_txn(TxnOptions()), 1);
    ASSERT_OK(put("a", "1"));
    ASSERT_OK(db->commit_txn(1));
    ASSERT_EQ(db->begin_txn(TxnOptions()), 2);
    ASSERT_OK(put("b", "2"));
    ASSERT_OK(db->commit_txn(2));
    ASSERT_EQ(db->begin_txn(TxnOptions()), 3);
    ASSERT_OK(put("c", "3"));
    open();

    ASSERT_EQ(get("a"), "1");
    ASSERT_EQ(get("b"), "2");
    ASSERT_EQ(get("c"), "NOT_FOUND");
}

TEST_F(RecoveryTests, VacuumRecovery)
{
    ASSERT_EQ(db->begin_txn(TxnOptions()), 1);
    std::vector<Record> committed;
    for (std::size_t i = 0; i < 1'000; ++i) {
        committed.emplace_back(Record {
            random.Generate(100).to_string(),
            random.Generate(100).to_string(),
        });
        ASSERT_OK(db->put(
            committed.back().key,
            committed.back().value));
    }
    ASSERT_OK(db->commit_txn(1));
    ASSERT_EQ(db->begin_txn(TxnOptions()), 2);

    for (std::size_t i = 0; i < 1'000; ++i) {
        ASSERT_OK(db->put(tools::integral_key(i), random.Generate(db_options.page_size)));
    }
    for (std::size_t i = 0; i < 1'000; ++i) {
        ASSERT_OK(db->erase(tools::integral_key(i)));
    }

    // Grow the database, then make freelist pages.
    for (std::size_t i = 0; i < 1'000; ++i) {
        ASSERT_OK(db->put(tools::integral_key(i), random.Generate(db_options.page_size)));
    }
    for (std::size_t i = 0; i < 1'000; ++i) {
        ASSERT_OK(db->erase(tools::integral_key(i)));
    }
    // Shrink the database.
    ASSERT_OK(db->vacuum());

    // Grow the database again. This time, it will look like we need to write image records
    // for the new pages, even though they are already in the WAL.
    for (std::size_t i = 0; i < 1'000; ++i) {
        ASSERT_OK(db->put(tools::integral_key(i), random.Generate(db_options.page_size)));
    }

    // Now reopen the database and roll the WAL.
    open();

    std::string result;
    for (const auto &[key, value] : committed) {
        ASSERT_OK(db->get(key, &result));
        ASSERT_EQ(result, value);
    }
    db_impl(db)->TEST_validate();
}

TEST_F(RecoveryTests, SanityCheck)
{
    std::map<std::string, std::string> map;
    const std::size_t N = 100;

    for (std::size_t i = 0; i < N; ++i) {
        const auto k = random.Generate(db_options.page_size * 2);
        const auto v = random.Generate(db_options.page_size * 4);
        map[k.to_string()] = v.to_string();
    }

    unsigned txn;
    for (std::size_t commit = 0; commit < map.size(); ++commit) {
        open();
        txn = db->begin_txn(TxnOptions());

        auto record = begin(map);
        for (std::size_t index = 0; record != end(map); ++index, ++record) {
            if (index == commit) {
                ASSERT_OK(db->commit_txn(txn));
                txn = db->begin_txn(TxnOptions());
            } else {
                ASSERT_OK(db->put(record->first, record->second));
            }
        }
        open();

        record = begin(map);
        for (std::size_t index = 0; record != end(map); ++index, ++record) {
            std::string value;
            if (index < commit) {
                ASSERT_OK(db->get(record->first, &value));
                ASSERT_EQ(value, record->second);
            } else {
                ASSERT_TRUE(db->get(record->first, &value).is_not_found());
            }
        }
        close();

        ASSERT_OK(DB::destroy(db_options, db_prefix));
    }
}

class RecoverySanityCheck
    : public RecoveryTestHarness<>,
      public testing::TestWithParam<std::tuple<std::string, tools::Interceptor::Type, int>>
{
public:
    RecoverySanityCheck()
        : interceptor_prefix(std::get<0>(GetParam()))
    {
        open();

        tools::RandomGenerator random(1'024 * 1'024 * 8);
        const std::size_t N = 10'000;

        for (std::size_t i = 0; i < N; ++i) {
            const auto k = random.Generate(db_options.page_size * 2);
            const auto v = random.Generate(db_options.page_size * 4);
            map[k.to_string()] = v.to_string();
        }
    }

    ~RecoverySanityCheck() override = default;

    auto SetUp() -> void override
    {
        m_txn = db->begin_txn(TxnOptions());
        auto record = begin(map);
        for (std::size_t index = 0; record != end(map); ++index, ++record) {
            ASSERT_OK(db->put(record->first, record->second));
            if (record->first.front() % 10 == 1) {
                ASSERT_OK(db->commit_txn(m_txn));
                m_txn = db->begin_txn(TxnOptions());
            }
        }
        ASSERT_OK(db->commit_txn(m_txn));
        m_txn = db->begin_txn(TxnOptions());

        COUNTING_INTERCEPTOR(interceptor_prefix, interceptor_type, interceptor_count);
    }

    auto validate() -> void
    {
        CLEAR_INTERCEPTORS();
        open();

        for (const auto &[k, v] : map) {
            std::string value;
            ASSERT_OK(db->get(k, &value));
            ASSERT_EQ(value, v);
        }
    }

    std::string interceptor_prefix;
    tools::Interceptor::Type interceptor_type {std::get<1>(GetParam())};
    int interceptor_count {std::get<2>(GetParam())};
    std::map<std::string, std::string> map;
    unsigned m_txn;
};

TEST_P(RecoverySanityCheck, FailureWhileRunning)
{
    for (const auto &[k, v] : map) {
        auto s = db->erase(k);
        if (!s.is_ok()) {
            assert_special_error(s);
            break;
        }
    }
    if (db->status().is_ok()) {
        (void)db->vacuum();
    }
    assert_special_error(db->status());

    validate();
}

// TODO: Find some way to determine if an error occurred during the destructor. It happens in each
//       instance except for when we attempt to fail due to a WAL write error, since the WAL is not
//       written during the erase/recovery routine.
TEST_P(RecoverySanityCheck, FailureDuringClose)
{
    // The final transaction committed successfully, so the data we added should persist.
    close();

    validate();
}

TEST_P(RecoverySanityCheck, FailureDuringCloseWithUncommittedUpdates)
{
    while (db->status().is_ok()) {
        (void)db->put(random.Generate(16), random.Generate(100));
    }

    close();
    validate();
}

INSTANTIATE_TEST_SUITE_P(
    RecoverySanityCheck,
    RecoverySanityCheck,
    ::testing::Values(
        std::make_tuple("./test", tools::Interceptor::kRead, 0),
        std::make_tuple("./test", tools::Interceptor::kRead, 1),
        std::make_tuple("./test", tools::Interceptor::kRead, 5),
        std::make_tuple("./wal", tools::Interceptor::kRead, 0),
        std::make_tuple("./wal", tools::Interceptor::kRead, 1),
        std::make_tuple("./wal", tools::Interceptor::kRead, 5),
        std::make_tuple("./wal", tools::Interceptor::kWrite, 0),
        std::make_tuple("./wal", tools::Interceptor::kWrite, 1),
        std::make_tuple("./wal", tools::Interceptor::kWrite, 5)));

class OpenErrorTests : public RecoverySanityCheck
{
public:
    ~OpenErrorTests() override = default;

    auto SetUp() -> void override
    {
        RecoverySanityCheck::SetUp();
        const auto saved_count = interceptor_count;
        interceptor_count = 0;

        // Should fail on the first syscall given by "std::get<1>(GetParam())".
        close();

        interceptor_count = saved_count;
    }
};

TEST_P(OpenErrorTests, FailureDuringOpen)
{
    assert_special_error(open_with_status());
    validate();
}

INSTANTIATE_TEST_SUITE_P(
    OpenErrorTests,
    OpenErrorTests,
    ::testing::Values(
        std::make_tuple("./test", tools::Interceptor::kRead, 0),
        std::make_tuple("./test", tools::Interceptor::kRead, 1),
        std::make_tuple("./test", tools::Interceptor::kRead, 2)));

class DataLossTests
    : public RecoveryTestHarness<tools::DataLossEnv>,
      public testing::TestWithParam<std::size_t>
{
public:
    const std::size_t kCommitInterval = GetParam();

    ~DataLossTests() override = default;

    auto close() -> void override
    {
        // Hack to force an error to occur. The DB won't attempt to recover on close()
        // in this case. It will have to wait until open().
        //        const_cast<DBState &>(db_impl(db)->TEST_state()).status = special_error();

        RecoveryTestHarness::close();

        drop_unsynced_wal_data();
        drop_unsynced_db_data();
    }

    auto drop_unsynced_wal_data() const -> void
    {
        env->drop_after_last_sync(kWalFilename);
    }

    auto drop_unsynced_db_data() const -> void
    {
        env->drop_after_last_sync(kFilename);
    }
};

TEST_P(DataLossTests, LossBeforeFirstCheckpoint)
{
    for (std::size_t i = 0; i < kCommitInterval; ++i) {
        ASSERT_OK(db->put(tools::integral_key(i), "value"));
    }
    open();
}

TEST_P(DataLossTests, RecoversLastCheckpoint)
{
    auto txn = db->begin_txn(TxnOptions());
    for (std::size_t i = 0; i < kCommitInterval * 10; ++i) {
        if (i % kCommitInterval == 0) {
            ASSERT_OK(db->commit_txn(txn));
        }
        ASSERT_OK(db->put(tools::integral_key(i), tools::integral_key(i)));
    }
    open();

    for (std::size_t i = 0; i < kCommitInterval * 9; ++i) {
        std::string value;
        ASSERT_OK(db->get(tools::integral_key(i), &value));
        ASSERT_EQ(value, tools::integral_key(i));
    }
}

TEST_P(DataLossTests, LongTransaction)
{
    auto txn = db->begin_txn(TxnOptions());
    for (std::size_t i = 0; i < kCommitInterval * 10; ++i) {
        ASSERT_OK(db->put(tools::integral_key(i), tools::integral_key(i)));
        if (i % kCommitInterval == kCommitInterval - 1) {
            ASSERT_OK(db->commit_txn(txn));
            ASSERT_EQ(db->begin_txn(TxnOptions()), 1);
        }
    }

    for (std::size_t i = 0; i < kCommitInterval * 10; ++i) {
        ASSERT_OK(db->erase(tools::integral_key(i)));
    }
    ASSERT_OK(db->vacuum());

    open();

    for (std::size_t i = 0; i < kCommitInterval * 10; ++i) {
        std::string value;
        ASSERT_OK(db->get(tools::integral_key(i), &value));
        ASSERT_EQ(value, tools::integral_key(i));
    }
}

INSTANTIATE_TEST_SUITE_P(
    DataLossTests,
    DataLossTests,
    ::testing::Values(1, 10, 100, 1'000, 10'000));

} // namespace calicodb
