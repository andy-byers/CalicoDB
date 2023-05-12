// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/db.h"
#include "tools.h"
#include "unit_tests.h"

namespace calicodb
{

template <class EnvType = tools::TestEnv>
class RecoveryTestHarness : public EnvTestHarness<EnvType>
{
public:
    using Base = EnvTestHarness<EnvType>;

    RecoveryTestHarness()
    {
        db_options.wal_filename = kWalFilename;
        db_options.cache_size = kPageSize * kMinFrameCount;
        db_options.env = &Base::env();

        // TODO: Running these in sync mode right now, it's easier to tell how the DB should
        //       look. Should test not sync mode as well. Will likely lose more than 1 transaction,
        //       but the DB should not become corrupted.
        db_options.sync = true;

        open();
    }

    auto close_impl() -> void
    {
        delete table;
        table = nullptr;
        delete txn;
        txn = nullptr;
        delete db;
        db = nullptr;
    }
    ~RecoveryTestHarness() override
    {
        close_impl();
    }
    virtual auto close() -> void
    {
        close_impl();
    }

    auto open_with_status(Options *options = nullptr) -> Status
    {
        close();
        Options opts = db_options;
        if (options != nullptr) {
            opts = *options;
        }
        if (opts.env == nullptr) {
            opts.env = &Base::env();
        }
        CALICODB_TRY(DB::open(opts, kDBFilename, db));
        CALICODB_TRY(db->new_txn(true, txn));
        return txn->new_table(TableOptions(), "table", table);
    }

    auto open(Options *options = nullptr) -> void
    {
        ASSERT_OK(open_with_status(options));
    }

    auto put(const std::string &k, const std::string &v) const -> Status
    {
        return table->put(k, v);
    }

    auto get(const std::string &k) const -> std::string
    {
        std::string result;
        const auto s = table->get(k, &result);
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
        EXPECT_OK(Base::env().file_size(fname, result));
        return result;
    }

    tools::RandomGenerator random;
    Options db_options;
    DB *db = nullptr;
    Txn *txn = nullptr;
    Table *table = nullptr;
};

class RecoveryTests
    : public RecoveryTestHarness<>,
      public testing::Test
{
protected:
    static constexpr std::size_t kN = 500;
};

TEST_F(RecoveryTests, DetectsCorruptedIdentifier)
{
    tools::RandomGenerator random;
    tools::fill_db(*table, random, 1'000);
    ASSERT_OK(txn->commit());

    delete table;
    table = nullptr;
    delete txn;
    txn = nullptr;

    ASSERT_OK(db->checkpoint(true));

    auto dbfile = tools::read_file_to_string(env(), kDBFilename);
    ++dbfile[0];
    tools::write_string_to_file(env(), kDBFilename, dbfile, 0);

    Status s;
    ASSERT_TRUE((s = db->new_txn(true, txn)).is_invalid_argument())
        << "expected corruption status but got " << s.to_string();
}

TEST_F(RecoveryTests, DetectsCorruptedRoot)
{
    ASSERT_OK(txn->commit());

    delete table;
    table = nullptr;
    delete txn;
    txn = nullptr;

    ASSERT_OK(db->checkpoint(true));

    auto root = tools::read_file_to_string(env(), kDBFilename);
    root.resize(kPageSize);
    ++root.back(); // Root ID is right at the end of the page.
    tools::write_string_to_file(env(), kDBFilename, root, 0);

    close();

    Status s;
    ASSERT_TRUE(open_with_status().is_corruption())
        << "expected corruption status but got " << s.to_string();
}

TEST_F(RecoveryTests, NormalShutdown)
{
    ASSERT_EQ(num_wal_frames(), 0);
    ASSERT_OK(put("a", "1"));
    ASSERT_OK(txn->commit());
    ASSERT_OK(put("b", "2"));
    ASSERT_OK(txn->commit());
    ASSERT_OK(put("c", "3"));
    ASSERT_OK(txn->commit());
    ASSERT_GE(num_wal_frames(), 3);
    close();

    ASSERT_FALSE(Base::env().file_exists(kWalFilename));
}

TEST_F(RecoveryTests, RollbackA)
{
    std::string prefix;
    for (std::size_t i = 0; i < kN; ++i) {
        ASSERT_OK(put(prefix + "a", "1"));
        ASSERT_OK(put(prefix + "b", "2"));
        ASSERT_OK(put(prefix + "c", "3"));
        ASSERT_OK(txn->commit());

        ASSERT_OK(put(prefix + "c", "X"));
        ASSERT_OK(put(prefix + "d", "4"));
        if (i & 1) {
            // If rollback_txn() is not called, rollback happens automatically when the Txn
            // is deleted.
            txn->rollback();
        }
        open();

        ASSERT_EQ(get(prefix + "a"), "1");
        ASSERT_EQ(get(prefix + "b"), "2");
        ASSERT_EQ(get(prefix + "c"), "3");
        ASSERT_EQ(get(prefix + "d"), "NOT_FOUND");
        prefix += '_';
    }
}

TEST_F(RecoveryTests, RollbackB)
{
    for (std::size_t iteration = 0; iteration < 3; ++iteration) {
        // Keep these changes.
        const auto base = iteration * kN;
        for (std::size_t i = 0; i < kN; ++i) {
            const auto key = tools::integral_key(base + i);
            ASSERT_OK(put(key, key));
        }
        ASSERT_OK(txn->commit());

        // Rollback these changes.
        for (std::size_t i = 0; i < kN; ++i) {
            ASSERT_OK(table->erase(tools::integral_key(base + i)));
        }
        for (std::size_t i = kN; i < kN * 2; ++i) {
            ASSERT_OK(put(tools::integral_key(base + i), "42"));
        }

        // Every possible combination these 2 calls should produce the same
        // outcome: rollback of the current transaction.
        if (iteration <= 1) {
            txn->rollback();
        }
        if (iteration >= 1) {
            open();
        }

        // Only the committed changes should persist.
        for (std::size_t i = 0; i < kN * 2; ++i) {
            const auto key = tools::integral_key(base + i);
            ASSERT_EQ(get(key), i < kN ? key : "NOT_FOUND");
        }
    }
}

TEST_F(RecoveryTests, RollbackC)
{
    auto records = tools::fill_db(*table, random, kN);
    ASSERT_OK(txn->commit());
    open();

    tools::fill_db(*table, random, kN);
    txn->rollback();

    for (const auto &[key, value] : records) {
        ASSERT_EQ(get(key), value);
    }
}

TEST_F(RecoveryTests, RollbackD)
{
    auto records = tools::fill_db(*table, random, kN);
    ASSERT_OK(txn->commit());
    open();
    const auto actual = tools::read_file_to_string(*m_env, kDBFilename).substr(kPageSize * 2, kPageSize);

    for (std::size_t iteration = 0; iteration < 3; ++iteration) {
        for (std::size_t i = 0; i < kN; ++i) {
            // Same keys each time. Since what we did before was rolled back, these
            // keys don't exist anyway.
            const auto key = tools::integral_key(i);
            ASSERT_OK(put(key, key));
        }
        txn->rollback();

        if (iteration & 1) {
            open();
        }
        for (const auto &[key, value] : records) {
            ASSERT_EQ(get(key), value);
        }
    }
}

TEST_F(RecoveryTests, VacuumRecovery)
{
    const auto committed = tools::fill_db(*table, random, 50);
    ASSERT_OK(txn->commit());

    // Grow the database, then make freelist pages.
    for (std::size_t i = 0; i < 10; ++i) {
        ASSERT_OK(table->put(tools::integral_key(i), random.Generate(kPageSize)));
    }
    for (std::size_t i = 0; i < 10; ++i) {
        ASSERT_OK(table->erase(tools::integral_key(i)));
    }
    std::cerr << table_impl(table)->tree()->TEST_to_string() << "\n\n";
    tools::print_references(const_cast<Pager &>(db_impl(db)->TEST_pager()));

    // Shrink the database.
    ASSERT_OK(txn->vacuum());

    std::cerr << "\n\n";
    tools::print_references(const_cast<Pager &>(db_impl(db)->TEST_pager()));
    std::cerr << table_impl(table)->tree()->TEST_to_string() << "\n\n";

    // Grow the database again.
    for (std::size_t i = 0; i < 10; ++i) {
        ASSERT_OK(table->put(tools::integral_key(i), random.Generate(kPageSize)));
    }

    // Now reopen the database and roll the WAL.
    open();

    std::string result;
    for (const auto &[key, value] : committed) {
        ASSERT_OK(table->get(key, &result));
        ASSERT_EQ(result, value);
    }
}

TEST_F(RecoveryTests, SanityCheck)
{
    std::map<std::string, std::string> map;

    for (std::size_t i = 0; i < kN; ++i) {
        const auto k = random.Generate(kPageSize * 2);
        const auto v = random.Generate(kPageSize * 4);
        map[k.to_string()] = v.to_string();
    }

    for (std::size_t commit = 0; commit < map.size(); ++commit) {
        open();

        auto record = begin(map);
        for (std::size_t index = 0; record != end(map); ++index, ++record) {
            if (index == commit) {
                ASSERT_OK(txn->commit());
            } else {
                ASSERT_OK(table->put(record->first, record->second));
            }
        }
        open();

        record = begin(map);
        for (std::size_t index = 0; record != end(map); ++index, ++record) {
            std::string value;
            if (index < commit) {
                ASSERT_OK(table->get(record->first, &value));
                ASSERT_EQ(value, record->second);
            } else {
                ASSERT_TRUE(table->get(record->first, &value).is_not_found());
            }
        }
        close();

        ASSERT_OK(DB::destroy(db_options, kDBFilename));
    }
}

class RecoverySanityCheck
    : public RecoveryTestHarness<>,
      public testing::TestWithParam<std::tuple<std::string, tools::SyscallType, int>>
{
public:
    explicit RecoverySanityCheck()
        : interceptor_prefix(std::get<0>(GetParam()))
    {
        open();

        tools::RandomGenerator random(1'024 * 1'024 * 8);
        const std::size_t N = 10'000;

        for (std::size_t i = 0; i < N; ++i) {
            const auto k = random.Generate(kPageSize * 2);
            const auto v = random.Generate(kPageSize * 4);
            map[k.to_string()] = v.to_string();
        }
    }

    ~RecoverySanityCheck() override = default;

    auto SetUp() -> void override
    {
        auto record = begin(map);
        for (std::size_t index = 0; record != end(map); ++index, ++record) {
            ASSERT_OK(table->put(record->first, record->second));
            if (record->first.front() % 10 == 1) {
                ASSERT_OK(txn->commit());
            }
        }
        ASSERT_OK(txn->commit());
        COUNTING_INTERCEPTOR(interceptor_prefix, interceptor_type, interceptor_count);
    }

    auto validate() -> void
    {
        CLEAR_INTERCEPTORS();
        open();

        for (const auto &[k, v] : map) {
            std::string value;
            ASSERT_OK(table->get(k, &value));
            ASSERT_EQ(value, v);
        }
    }

    std::string interceptor_prefix;
    tools::SyscallType interceptor_type = std::get<1>(GetParam());
    int interceptor_count = std::get<2>(GetParam());
    std::map<std::string, std::string> map;
};

TEST_P(RecoverySanityCheck, FailureWhileRunning)
{
    for (const auto &[k, v] : map) {
        auto s = table->erase(k);
        if (!s.is_ok()) {
            assert_special_error(s);
            break;
        }
    }
    if (txn->status().is_ok()) {
        (void)txn->vacuum();
    }
    assert_special_error(txn->status());

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
    while (txn->status().is_ok()) {
        (void)table->put(random.Generate(16), random.Generate(100));
    }

    close();
    validate();
}

INSTANTIATE_TEST_SUITE_P(
    RecoverySanityCheck,
    RecoverySanityCheck,
    ::testing::Values(
        std::make_tuple(kWalFilename, tools::kSyscallRead, 0),
        std::make_tuple(kWalFilename, tools::kSyscallRead, 1),
        std::make_tuple(kWalFilename, tools::kSyscallRead, 5),
        std::make_tuple(kWalFilename, tools::kSyscallWrite, 0),
        std::make_tuple(kWalFilename, tools::kSyscallWrite, 1),
        std::make_tuple(kWalFilename, tools::kSyscallWrite, 5)));

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
        std::make_tuple(kDBFilename, tools::kSyscallRead, 0)));

} // namespace calicodb
