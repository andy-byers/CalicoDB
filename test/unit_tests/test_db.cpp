// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "db_impl.h"
#include "header.h"
#include "logging.h"
#include "tools.h"
#include "tree.h"
#include "unit_tests.h"
#include "wal.h"
#include <atomic>
#include <filesystem>
#include <gtest/gtest.h>

namespace calicodb
{

class DBTests : public testing::Test
{
protected:
    static constexpr auto kDBDir = "/tmp/calicodb_test";
    static constexpr auto kDBName = "/tmp/calicodb_test/testdb";
    static constexpr auto kWALName = "/tmp/calicodb_test/testdb-wal";
    static constexpr auto kShmName = "/tmp/calicodb_test/testdb-shm";
    static constexpr auto kAltWALName = "/tmp/calicodb_test/testwal";
    static constexpr std::size_t kMaxRounds = 1'000;

    explicit DBTests()
        : m_env(Env::default_env())
    {
        std::filesystem::remove_all(kDBDir);
        std::filesystem::create_directory(kDBDir);
    }

    ~DBTests() override
    {
        delete m_db;
        std::filesystem::remove_all(kDBDir);
    }

    auto SetUp() -> void override
    {
        ASSERT_OK(reopen_db(false));
    }

    [[nodiscard]] static auto make_kv(int kv, int round = 0) -> std::pair<std::string, std::string>
    {
        EXPECT_LE(0, kv);
        EXPECT_LE(0, round);
        static constexpr std::size_t kMaxKV = kPageSize * 2;
        const auto key_length = (round + 1) * kMaxKV / kMaxRounds;
        auto key_str = tools::integral_key(kv);
        const auto val_length = kMaxKV - key_length;
        auto val_str = number_to_string(kv);
        if (val_str.size() < val_length) {
            val_str.resize(kPageSize / 4 - val_str.size(), '0');
        }
        return {key_str, val_str};
    }

    [[nodiscard]] static auto put(Table &table, int kv, int round = 0) -> Status
    {
        const auto [k, v] = make_kv(kv, round);
        return table.put(k, v);
    }
    [[nodiscard]] static auto put(Txn &txn, const TableOptions &options, const std::string &tbname, int kv, int round = 0) -> Status
    {
        Table *table;
        auto s = txn.new_table(options, tbname, table);
        if (s.is_ok()) {
            s = put(*table, kv, round);
            delete table;
        }
        return s;
    }

    [[nodiscard]] static auto put_range(Table &table, int kv1, int kv2, int round = 0) -> Status
    {
        Status s;
        for (int kv = kv1; s.is_ok() && kv < kv2; ++kv) {
            s = put(table, kv, round);
        }
        return s;
    }
    [[nodiscard]] static auto put_range(Txn &txn, const TableOptions &options, const std::string &tbname, int kv1, int kv2, int round = 0) -> Status
    {
        Table *table;
        auto s = txn.new_table(options, tbname, table);
        if (s.is_ok()) {
            s = put_range(*table, kv1, kv2, round);
            delete table;
        }
        return s;
    }

    [[nodiscard]] static auto erase(Table &table, int kv, int round = 0) -> Status
    {
        const auto [k, _] = make_kv(kv, round);
        return table.erase(k);
    }
    [[nodiscard]] static auto erase(Txn &txn, const TableOptions &options, const std::string &tbname, int kv, int round = 0) -> Status
    {
        Table *table;
        auto s = txn.new_table(options, tbname, table);
        if (s.is_ok()) {
            s = erase(*table, kv, round);
            delete table;
        }
        return s;
    }

    [[nodiscard]] static auto erase_range(Table &table, int kv1, int kv2, int round = 0) -> Status
    {
        Status s;
        for (int kv = kv1; s.is_ok() && kv < kv2; ++kv) {
            s = erase(table, kv, round);
        }
        return s;
    }
    [[nodiscard]] static auto erase_range(Txn &txn, const TableOptions &options, const std::string &tbname, int kv1, int kv2, int round = 0) -> Status
    {
        Table *table;
        auto s = txn.new_table(options, tbname, table);
        if (s.is_ok()) {
            s = erase_range(*table, kv1, kv2, round);
            delete table;
        }
        return s;
    }

    [[nodiscard]] static auto check(Table &table, int kv, bool exists, int round = 0) -> Status
    {
        std::string result;
        const auto [k, v] = make_kv(kv, round);
        auto s = table.get(k, &result);
        if (s.is_ok()) {
            EXPECT_TRUE(exists);
            U64 n;
            Slice slice(result);
            EXPECT_TRUE(consume_decimal_number(slice, &n));
            EXPECT_EQ(kv, n);
        } else if (s.is_not_found()) {
            EXPECT_FALSE(exists);
        }
        return s;
    }
    [[nodiscard]] static auto check(Txn &txn, const TableOptions &options, const std::string &tbname, int kv, bool exists, int round = 0) -> Status
    {
        Table *table;
        auto s = txn.new_table(options, tbname, table);
        if (s.is_ok()) {
            s = check(*table, kv, exists, round);
            delete table;
        }
        return s;
    }

    [[nodiscard]] static auto check_range(Table &table, int kv1, int kv2, bool exists, int round = 0) -> Status
    {
        auto *c = table.new_cursor();
        // Run some extra seek*() calls.
        if (kv1 & 1) {
            c->seek_first();
        } else {
            c->seek_last();
        }
        if (c->status().is_io_error()) {
            return c->status();
        }
        if (exists) {
            for (int kv = kv1; kv < kv2; ++kv) {
                const auto [k, v] = make_kv(kv, round);
                if (kv == kv1) {
                    c->seek(k);
                }
                if (c->is_valid()) {
                    EXPECT_EQ(k, c->key().to_string());
                    EXPECT_EQ(v, c->value().to_string());
                } else {
                    EXPECT_TRUE((c->status().is_io_error()));
                    return c->status();
                }
                c->next();
            }
            for (int kv = kv2 - 1; kv >= kv1; --kv) {
                const auto [k, v] = make_kv(kv, round);
                if (kv == kv2 - 1) {
                    c->seek(k);
                }
                if (c->is_valid()) {
                    EXPECT_EQ(k, c->key());
                    EXPECT_EQ(v, c->value());
                } else {
                    EXPECT_TRUE((c->status().is_io_error()));
                    return c->status();
                }
                c->previous();
            }
        } else {
            for (int kv = kv1; kv < kv2; ++kv) {
                const auto [k, v] = make_kv(kv, round);
                c->seek(k);
                if (c->is_valid()) {
                    EXPECT_NE(k, c->key().to_string());
                } else if (!c->status().is_not_found()) {
                    EXPECT_TRUE((c->status().is_io_error()));
                    return c->status();
                }
                ++kv;
            }
        }
        return Status::ok();
    }
    [[nodiscard]] static auto check_range(Txn &txn, const TableOptions &options, const std::string &tbname, int kv1, int kv2, bool exists, int round = 0) -> Status
    {
        Table *table;
        auto s = txn.new_table(options, tbname, table);
        if (s.is_ok()) {
            s = check_range(*table, kv1, kv2, exists, round);
            delete table;
        }
        return s;
    }

    enum Config {
        kDefault,
        kSyncMode,
        kUseAltWAL,
        kSmallCache,
        kMaxConfig,
    };
    [[nodiscard]] auto reopen_db(bool clear, Env *env = nullptr) -> Status
    {
        close_db();
        if (clear) {
            (void)DB::destroy(Options(), kDBName);
        }
        Options options;
        options.busy = &m_busy;
        options.env = env ? env : m_env;
        switch (m_config) {
            case kDefault:
                break;
            case kSyncMode:
                options.sync = true;
                break;
            case kUseAltWAL:
                options.wal_filename = kAltWALName;
                break;
            case kSmallCache:
                options.cache_size = 0;
                break;
            default:
                return Status::ok();
        }
        return DB::open(options, kDBName, m_db);
    }
    auto close_db() -> void
    {
        delete m_db;
        m_db = nullptr;
    }
    auto change_options(bool clear = false) -> bool
    {
        m_config = Config(m_config + 1);
        EXPECT_OK(reopen_db(clear));
        return kMaxConfig <= m_config;
    }

    [[nodiscard]] auto file_size(const std::string &filename) const -> std::size_t
    {
        std::size_t file_size;
        EXPECT_OK(m_env->file_size(filename, file_size));
        return file_size;
    }

    static constexpr std::size_t kMaxTables = 12;
    static constexpr const char kTableStr[kMaxTables + 2] = "TABLE_NAMING_";
    Config m_config = kDefault;
    Env *m_env = nullptr;
    DB *m_db = nullptr;

    class BusyHandlerStub : public BusyHandler
    {
    public:
        ~BusyHandlerStub() override = default;
        auto exec(unsigned) -> bool override
        {
            return true;
        }
    } m_busy;
};

TEST_F(DBTests, GetProperty)
{
    std::string value;
    ASSERT_TRUE(m_db->get_property("calicodb.stats", nullptr));
    ASSERT_TRUE(m_db->get_property("calicodb.stats", &value));
    ASSERT_FALSE(value.empty());
    ASSERT_FALSE(m_db->get_property("nonexistent", nullptr));
    ASSERT_FALSE(m_db->get_property("nonexistent", &value));
    ASSERT_TRUE(value.empty());
}

TEST_F(DBTests, ConvenienceFunctions)
{
    const auto *const_db = m_db;
    (void)db_impl(m_db)->TEST_pager();
    (void)db_impl(m_db)->TEST_state();
    db_impl(const_db);
    ASSERT_OK(m_db->update([](auto &txn) {
        const auto &const_txn = txn;
        txn_impl(&txn);
        txn_impl(&const_txn);
        Table *tbl;
        EXPECT_OK(txn.new_table(TableOptions(), "TABLE", tbl));
        const auto *const_tbl = tbl;
        (void)table_impl(tbl)->TEST_tree();
        table_impl(const_tbl);
        return Status::ok();
    }));
}

TEST_F(DBTests, NewTxn)
{
    for (int i = 0; i < 2; ++i) {
        for (int j = 0; j < 2; ++j) {
            Txn *txn1, *txn2 = reinterpret_cast<Txn *>(42);
            ASSERT_OK(m_db->new_txn(i == 0, txn1));
            ASSERT_NOK(m_db->new_txn(j == 0, txn2));
            ASSERT_EQ(nullptr, txn2);
            delete txn1;
        }
    }
}

TEST_F(DBTests, NewTable)
{
    ASSERT_OK(m_db->update([](auto &txn) {
        Table *table;
        TableOptions tbopt;
        tbopt.create_if_missing = false;
        EXPECT_NOK(txn.new_table(tbopt, "TABLE", table));
        tbopt.create_if_missing = true;
        EXPECT_OK(txn.new_table(tbopt, "TABLE", table));
        delete table;
        tbopt.error_if_exists = true;
        EXPECT_NOK(txn.new_table(tbopt, "TABLE", table));
        return Status::ok();
    }));
}

TEST_F(DBTests, TableBehavior)
{
    ASSERT_OK(m_db->update([](auto &txn) {
        Table *table;
        EXPECT_OK(txn.new_table(TableOptions(), "TABLE", table));
        // Table::put() should not accept an empty key.
        EXPECT_TRUE(table->put("", "value").is_invalid_argument());
        return Status::ok();
    }));
}

TEST_F(DBTests, ReadonlyTxn)
{
    ASSERT_OK(m_db->view([](auto &txn) {
        Table *table;
        // Cannot create a new table in a readonly transaction.
        EXPECT_NOK(txn.new_table(TableOptions(), "TABLE", table));
        return Status::ok();
    }));
    ASSERT_OK(m_db->update([](auto &txn) {
        Table *table;
        EXPECT_OK(txn.new_table(TableOptions(), "TABLE", table));
        delete table;
        return Status::ok();
    }));
    ASSERT_OK(m_db->view([](auto &txn) {
        EXPECT_TRUE(txn.vacuum().is_readonly());
        EXPECT_OK(txn.commit()); // NOOP, no changes to commit
        Table *table;
        EXPECT_OK(txn.new_table(TableOptions(), "TABLE", table));
        EXPECT_TRUE(table->put("k", "v").is_readonly());
        EXPECT_TRUE(table->erase("k").is_readonly());
        delete table;
        return Status::ok();
    }));
}

TEST_F(DBTests, UpdateThenView)
{
    int round = 0;
    do {
        TableOptions tbopt;
        tbopt.error_if_exists = true;
        for (int i = 0; i < 3; ++i) {
            ASSERT_OK(m_db->update([i, tbopt, round](auto &txn) {
                Table *table;
                auto s = txn.new_table(tbopt, kTableStr + i, table);
                if (s.is_ok()) {
                    s = put_range(*table, 0, 1'000, round);
                    if (s.is_ok()) {
                        s = erase_range(*table, 250, 750, round);
                    }
                }
                return s;
            }));
        }
        tbopt.error_if_exists = false;
        tbopt.create_if_missing = false;
        for (int i = 0; i < 3; ++i) {
            ASSERT_OK(m_db->view([round, i, tbopt](auto &txn) {
                Table *table;
                auto s = txn.new_table(tbopt, kTableStr + i, table);
                if (s.is_ok()) {
                    EXPECT_OK(check_range(*table, 0, 250, true, round));
                    EXPECT_OK(check_range(*table, 250, 750, false, round));
                    EXPECT_OK(check_range(*table, 750, 1'000, true, round));
                }
                return s;
            }));
        }
        ASSERT_OK(m_db->update([](auto &txn) {
            return txn.vacuum();
        }));
        ASSERT_OK(m_db->checkpoint(false));
        ++round;
    } while (change_options());
}

TEST_F(DBTests, RollbackUpdate)
{
    int round = 0;
    do {
        for (int i = 0; i < 3; ++i) {
            ASSERT_TRUE(m_db->update([i, round](auto &txn) {
                                Table *table;
                                auto s = txn.new_table(TableOptions(), kTableStr + i, table);
                                if (s.is_ok()) {
                                    s = put_range(*table, 0, 500, round);
                                    if (s.is_ok()) {
                                        // We have access to the Txn here, so we can actually call
                                        // Txn::commit() as many times as we want before we return.
                                        // The returned status determines whether to perform a final
                                        // commit before calling delete on the Txn.
                                        s = txn.commit();
                                        if (s.is_ok()) {
                                            s = put_range(*table, 500, 1'000, round);
                                            if (s.is_ok()) {
                                                // Cause the rest of the changes to be rolled back.
                                                return Status::not_found("42");
                                            }
                                        }
                                    }
                                }
                                return s;
                            })
                            .to_string() == "not found: 42");
        }
        for (int i = 0; i < 3; ++i) {
            ASSERT_OK(m_db->view([round, i](auto &txn) {
                Table *table;
                auto s = txn.new_table(TableOptions(), kTableStr + i, table);
                if (s.is_ok()) {
                    EXPECT_OK(check_range(*table, 0, 500, true, round));
                    EXPECT_OK(check_range(*table, 500, 1'000, false, round));
                }
                return s;
            }));
        }
        ASSERT_OK(m_db->checkpoint(false));
        ++round;
    } while (change_options());
}

TEST_F(DBTests, VacuumEmptyDB)
{
    ASSERT_OK(m_db->update([](auto &txn) {
        return txn.vacuum();
    }));
}

TEST_F(DBTests, CheckpointResize)
{
    ASSERT_OK(m_db->update([](auto &txn) {
        Table *table;
        auto s = txn.new_table(TableOptions(), "TABLE", table);
        if (s.is_ok()) {
            delete table;
        }
        return s;
    }));
    ASSERT_EQ(0, file_size(kDBName));

    ASSERT_OK(m_db->checkpoint(true));
    ASSERT_EQ(kPageSize * 3, file_size(kDBName));

    ASSERT_OK(m_db->update([](auto &txn) {
        auto s = txn.drop_table("TABLE");
        if (s.is_ok()) {
            s = txn.vacuum();
        }
        return s;
    }));
    ASSERT_EQ(kPageSize * 3, file_size(kDBName));

    // Txn::vacuum() never gets rid of the root database page, even if the whole database
    // is empty.
    ASSERT_OK(m_db->checkpoint(true));
    ASSERT_EQ(kPageSize, file_size(kDBName));
}

TEST(OldWalTests, HandlesOldWalFile)
{
    static constexpr auto kOldWal = "./testwal";

    File *oldwal;
    tools::FakeEnv env;
    ASSERT_OK(env.new_file(kOldWal, Env::kCreate, oldwal));
    ASSERT_OK(oldwal->write(42, ":3"));

    std::size_t file_size;
    ASSERT_OK(env.file_size(kOldWal, file_size));
    ASSERT_NE(0, file_size);
    delete oldwal;

    DB *db;
    Options dbopt;
    dbopt.env = &env;
    dbopt.wal_filename = kOldWal;
    ASSERT_OK(DB::open(dbopt, "./testdb", db));

    ASSERT_OK(env.file_size(kOldWal, file_size));
    ASSERT_EQ(0, file_size);
    delete db;
}

TEST(DestructionTests, OnlyDeletesCalicoDatabases)
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
    ASSERT_OK(file->write(0, "CalicoDB format 0"));
    ASSERT_TRUE(DB::destroy(options, "./testdb").is_invalid_argument());

    DB *db;
    std::filesystem::remove_all("./testdb");
    ASSERT_OK(DB::open(options, "./testdb", db));
    ASSERT_OK(DB::destroy(options, "./testdb"));

    delete db;
    delete file;
}

TEST(DestructionTests, OnlyDeletesCalicoWals)
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

class DBErrorTests : public DBTests
{
protected:
    static constexpr auto kErrorMessage = "I/O error: 42";
    static constexpr auto kAllSyscalls = (1 << tools::kNumSyscalls) - 1;
    static constexpr std::size_t kSavedCount = 1'000;

    explicit DBErrorTests()
    {
        m_test_env = new tools::TestEnv(*Env::default_env());
    }

    ~DBErrorTests() override
    {
        m_test_env->clear_interceptors();
        delete m_db;
        m_db = nullptr;
        delete m_test_env;
    }

    auto SetUp() -> void override
    {
        // Don't call DBTests::SetUp(). Defer calling DB::open() until try_reopen() is called.
    }

    [[nodiscard]] auto try_reopen(bool prefill, bool sync_mode = false) -> Status
    {
        if (sync_mode) {
            m_config = kSyncMode;
        } else {
            m_config = kDefault;
        }
        auto s = reopen_db(false, m_test_env);
        if (prefill && m_max_count == 0) {
            // The first time the DB is opened, add kSavedCount records to the WAL and
            // commit.
            s = m_db->update([](auto &txn) {
                return put_range(txn, TableOptions(), "saved", 0, kSavedCount);
            });
        }
        return s;
    }

    auto set_error(tools::SyscallType type) -> void
    {
        const tools::Interceptor interceptor(type, [this] {
            if (m_counter >= 0 && m_counter++ >= m_max_count) {
                return Status::io_error("42");
            }
            return Status::ok();
        });
        // Include system calls on every possible file.
        m_test_env->add_interceptor(kDBName, interceptor);
        m_test_env->add_interceptor(kWALName, interceptor);
        m_test_env->add_interceptor(kShmName, interceptor);
        m_test_env->add_interceptor(kAltWALName, interceptor);
    }
    auto reset_error(int max_count = -1) -> void
    {
        m_counter = 0;
        if (max_count >= 0) {
            m_max_count = max_count;
        } else {
            ++m_max_count;
        }
    }

    tools::TestEnv *m_test_env;
    Options m_options;
    int m_counter = 0;
    int m_max_count = 0;
};

TEST_F(DBErrorTests, Reads)
{
    ASSERT_OK(try_reopen(true));
    set_error(tools::kSyscallRead);

    for (;;) {
        auto s = m_db->view([](auto &txn) {
            auto s = check(txn, TableOptions(), "saved", 0, true);
            if (s.is_ok()) {
                s = check_range(txn, TableOptions(), "saved", 0, kSavedCount, true);
                if (s.is_ok()) {
                    s = check_range(txn, TableOptions(), "saved", kSavedCount, 2 * kSavedCount, false);
                }
            }
            EXPECT_OK(txn.status());
            return s;
        });
        if (s.is_ok()) {
            break;
        } else {
            ASSERT_EQ(kErrorMessage, s.to_string());
            reset_error();
        }
    }
    ASSERT_LT(0, m_max_count);
}

TEST_F(DBErrorTests, Writes)
{
    ASSERT_OK(try_reopen(true));
    set_error(tools::kSyscallWrite | tools::kSyscallSync);

    for (;;) {
        auto s = try_reopen(false);
        if (s.is_ok()) {
            s = m_db->update([](auto &txn) {
                auto s = put_range(txn, TableOptions(), "TABLE", 0, 1'000);
                EXPECT_EQ(s.to_string(), txn.status().to_string());
                return s;
            });
        }
        if (s.is_ok()) {
            break;
        } else {
            ASSERT_EQ(kErrorMessage, s.to_string());
            reset_error();
        }
    }
    m_test_env->clear_interceptors();
    ASSERT_OK(try_reopen(false));
    ASSERT_OK(m_db->view([](auto &txn) {
        return check_range(txn, TableOptions(), "TABLE", 0, kSavedCount, true);
    }));
    ASSERT_LT(0, m_max_count);
}

TEST_F(DBErrorTests, Checkpoint)
{
    // Add some records to the WAL and set the next syscall to fail. The checkpoint during
    // the close routine will fail.
    ASSERT_OK(try_reopen(true, true));
    set_error(kAllSyscalls);

    for (Status s;;) {
        s = try_reopen(false, true);
        if (s.is_ok()) {
            s = m_db->checkpoint(true);
            if (s.is_ok()) {
                m_test_env->clear_interceptors();
                break;
            }
        }
        ASSERT_EQ(kErrorMessage, s.to_string());
        reset_error();
    }

    ASSERT_OK(reopen_db(false));
    ASSERT_OK(m_db->view([](auto &txn) {
        return check_range(txn, TableOptions(), "saved", 0, kSavedCount, true);
    }));
    ASSERT_LT(0, m_max_count);
}

class DBOpenTests : public DBTests
{
protected:
    ~DBOpenTests() override = default;

    auto SetUp() -> void override
    {
        // Don't call DBTests::SetUp(). DB is opened in test body.
    }
};

TEST_F(DBOpenTests, CreatesMissingDb)
{
    Options options;
    options.error_if_exists = false;
    options.create_if_missing = true;
    ASSERT_OK(DB::open(options, kDBName, m_db));
    delete m_db;
    m_db = nullptr;

    options.create_if_missing = false;
    ASSERT_OK(DB::open(options, kDBName, m_db));
}

TEST_F(DBOpenTests, FailsIfMissingDb)
{
    Options options;
    options.create_if_missing = false;
    ASSERT_TRUE(DB::open(options, kDBName, m_db).is_invalid_argument());
}

TEST_F(DBOpenTests, FailsIfDbExists)
{
    Options options;
    options.create_if_missing = true;
    options.error_if_exists = true;
    ASSERT_OK(DB::open(options, kDBName, m_db));
    delete m_db;
    m_db = nullptr;

    options.create_if_missing = false;
    ASSERT_TRUE(DB::open(options, kDBName, m_db).is_invalid_argument());
}

class DBConcurrencyTests : public DBTests
{
protected:
    static constexpr std::size_t kRecordCount = 2;

    ~DBConcurrencyTests() override = default;

    auto SetUp() -> void override
    {
    }

    // Reader task invariants:
    // 1. If the table named "TABLE" exists, it contains kRecordCount records
    // 2. Record keys are monotonically increasing integers starting from 0, serialized
    //    using tools::integral_key()
    // 3. Each record value is another such serialized integer, however, each value is
    //    identical
    // 4. The record value read by a reader must never decrease between runs
    [[nodiscard]] static auto reader(DB &db, U64 &latest) -> Status
    {
        return db.view([&latest](auto &txn) {
            Table *tbl;
            auto s = txn.new_table(TableOptions(), "TABLE", tbl);
            if (s.is_invalid_argument()) {
                // Writer hasn't created the table yet.
                return Status::ok();
            } else if (!s.is_ok()) {
                return s;
            }
            for (std::size_t i = 0; i < kRecordCount; ++i) {
                std::string value;
                // If the table exists, then it must contain kRecordCount records (the first writer to run
                // makes sure of this).
                s = tbl->get(tools::integral_key(i), &value);
                if (s.is_ok()) {
                    U64 result;
                    Slice slice(value);
                    EXPECT_TRUE(consume_decimal_number(slice, &result));
                    if (i) {
                        EXPECT_EQ(latest, result);
                    } else {
                        if (latest > result) {
                            std::cerr << table_impl(tbl)->TEST_tree().TEST_to_string() << '\n';
                        }
                        EXPECT_LE(latest, result);
                        latest = result;
                    }
                } else {
                    break;
                }
            }
            delete tbl;
            return s;
        });
    }

    // Writer tasks set up invariants on the DB for the reader to check. Each writer
    // either creates or increases kRecordCount records in a table named "TABLE". The
    // first writer to run creates the table.
    [[nodiscard]] static auto writer(DB &db) -> Status
    {
        return db.update([](auto &txn) {
            Table *tbl = nullptr;
            auto s = txn.new_table(TableOptions(), "TABLE", tbl);
            for (std::size_t i = 0; s.is_ok() && i < kRecordCount; ++i) {
                U64 result = 1;
                std::string value;
                s = tbl->get(tools::integral_key(i), &value);
                if (s.is_not_found()) {
                    s = Status::ok();
                } else if (s.is_ok()) {
                    Slice slice(value);
                    EXPECT_TRUE(consume_decimal_number(slice, &result));
                    ++result;
                } else {
                    break;
                }
                s = tbl->put(tools::integral_key(i), tools::integral_key(result));
            }
            EXPECT_OK(s);
            delete tbl;
            return s;
        });
    }

    // Checkpointers just run a single checkpoint on the DB. This should not interfere with the
    // logical contents of the database in any way.
    [[nodiscard]] static auto checkpointer(DB &db, bool reset = false) -> Status
    {
        return db.checkpoint(reset);
    }

    [[nodiscard]] auto new_connection(bool sync, DB *&db_out) -> Status
    {
        Options options;
        options.env = m_env;
        options.sync = sync;
        options.busy = &m_busy;

        return DB::open(options, kDBName, db_out);
    }

    auto validate(U64 value) -> void
    {
        ASSERT_OK(reader(*m_db, value));
    }

    struct ConsistencyCheckParam {
        std::size_t read_count = 0;
        std::size_t write_count = 0;
        std::size_t ckpt_count = 0;
        U64 start_value = 0;
        bool ckpt_reset = false;
        bool ckpt_before = false;
    };
    auto consistency_check_step(const ConsistencyCheckParam &param) -> void
    {
        const auto total = param.read_count + param.write_count + param.ckpt_count;
        std::vector<U64> latest(param.read_count, param.start_value);
        std::vector<std::thread> threads;
        std::atomic<int> count(0);
        threads.reserve(total);
        for (std::size_t i = 0; i < total; ++i) {
            threads.emplace_back([i, param, total, &latest, &count, this] {
                const auto &[nrd, nwr, nck, _1, reset, _2] = param;

                DB *db;
                ASSERT_OK(new_connection(false, db));

                ++count;
                while (count.load() < total) {
                    std::this_thread::yield();
                }

                if (i < nrd) {
                    ASSERT_OK(reader(*db, latest[i])) << "reader (" << i << ") failed";
                } else if (i < nrd + nwr) {
                    Status s;
                    while ((s = writer(*db)).is_busy()) {
                    }
                    ASSERT_OK(s) << "writer (" << i << ") failed";
                } else {
                    Status s;
                    while ((s = checkpointer(*db, reset)).is_busy()) {
                    }
                    ASSERT_OK(s) << (reset ? "reset" : "passive") << " checkpointer (" << i << ") failed";
                }
                delete db;
            });
        }
        for (auto &thread : threads) {
            thread.join();
        }
    }
    auto run_consistency_check(const ConsistencyCheckParam &param) -> void
    {
        // Start with a fresh DB. Unlinks old database files.
        ASSERT_OK(reopen_db(true));
        for (std::size_t i = 0; i < param.start_value; ++i) {
            ASSERT_OK(writer(*m_db));
        }
        if (param.ckpt_before) {
            ASSERT_OK(m_db->checkpoint(param.ckpt_reset));
        }
        auto child_param = param;
        static constexpr std::size_t kNumRounds = 5;
        for (std::size_t i = 0; i < kNumRounds; ++i) {
            consistency_check_step(param);
            // The main connection should be able to see everything written by the
            // writer threads.
            child_param.start_value += param.write_count;
            validate(child_param.start_value);
        }
    }
};

TEST_F(DBConcurrencyTests, Reader1)
{
    run_consistency_check({100, 0, 0, 0, false, false});
    run_consistency_check({100, 0, 0, 10, false, false});
    run_consistency_check({100, 0, 0, 10, false, true});
}

TEST_F(DBConcurrencyTests, Reader2)
{
    run_consistency_check({100, 0, 10, 0, false, false});
    run_consistency_check({100, 0, 10, 10, false, false});
    run_consistency_check({100, 0, 10, 0, true, false});
    run_consistency_check({100, 0, 10, 10, true, false});
}

TEST_F(DBConcurrencyTests, Writer1)
{
    run_consistency_check({100, 1, 0, 0, false, false});
    run_consistency_check({100, 1, 0, 10, false, false});
    run_consistency_check({100, 1, 0, 10, false, true});
}

TEST_F(DBConcurrencyTests, Writer2)
{
    run_consistency_check({100, 1, 10, 0, false, false});
    run_consistency_check({100, 1, 10, 10, false, false});
    run_consistency_check({100, 1, 10, 0, true, false});
    run_consistency_check({100, 1, 10, 10, true, false});
}

TEST_F(DBConcurrencyTests, Checkpointer1)
{
    run_consistency_check({100, 20, 0, 0, false, false});
    run_consistency_check({100, 20, 0, 10, false, false});
    run_consistency_check({100, 20, 0, 10, false, true});
}

TEST_F(DBConcurrencyTests, Checkpointer2)
{
    run_consistency_check({100, 10, 10, 0, false, false});
    run_consistency_check({100, 10, 10, 10, false, false});
    run_consistency_check({100, 10, 10, 0, true, false});
    run_consistency_check({100, 10, 10, 10, true, false});
}

class DBTransactionTests : public DBErrorTests
{
protected:
    explicit DBTransactionTests() = default;

    ~DBTransactionTests() override = default;
};

TEST_F(DBTransactionTests, ReadMostRecentSnapshot)
{
    U64 key_limit = 0;
    auto should_exist = false;
    ASSERT_OK(try_reopen(true));
    const auto intercept = [this, &key_limit, &should_exist] {
        DB *db;
        Options options;
        options.env = m_test_env;
        EXPECT_OK(DB::open(options, kDBName, db));
        auto s = db->view([key_limit](auto &txn) {
            return check_range(txn, TableOptions(), "TABLE", 0, key_limit * 10, true);
        });
        if (!should_exist && s.is_invalid_argument()) {
            s = Status::ok();
        }
        delete db;
        return s;
    };
    m_test_env->add_interceptor(kWALName, tools::Interceptor(tools::kSyscallWrite, intercept));
    (void)m_db->update([&should_exist, &key_limit](auto &txn) {
        for (std::size_t i = 0; i < 50; ++i) {
            EXPECT_OK(put_range(txn, TableOptions(), "TABLE", i * 10, (i + 1) * 10));
            EXPECT_OK(txn.commit());
            should_exist = true;
            key_limit = i + 1;
        }
        return Status::ok();
    });
}

TEST_F(DBTransactionTests, IgnoresFutureVersions)
{
    static constexpr U64 kN = 300;
    auto has_open_db = false;
    U64 n = 0;

    ASSERT_OK(try_reopen(true));
    const auto intercept = [this, &has_open_db, &n] {
        if (has_open_db) {
            // Prevent this callback from being called by itself.
            return Status::ok();
        }
        DB *db;
        Options options;
        options.env = m_test_env;
        has_open_db = true;
        EXPECT_OK(DB::open(options, kDBName, db));
        EXPECT_OK(db->update([n](auto &txn) {
            return put_range(txn, TableOptions(), "TABLE", kN * n, kN * (n + 1));
        }));
        delete db;
        has_open_db = false;
        ++n;
        return Status::ok();
    };
    ASSERT_OK(m_db->update([](auto &txn) {
        return put_range(txn, TableOptions(), "TABLE", 0, kN);
    }));
    m_test_env->add_interceptor(kWALName, tools::Interceptor(tools::kSyscallRead, intercept));
    (void)m_db->view([&n](auto &txn) {
        for (std::size_t i = 0; i < kN; ++i) {
            EXPECT_OK(check_range(txn, TableOptions(), "TABLE", 0, kN, true));
            EXPECT_OK(check_range(txn, TableOptions(), "TABLE", kN, kN * (n + 1), false));
        }
        return Status::ok();
    });
}

class DBCheckpointTests : public DBErrorTests
{
protected:
    explicit DBCheckpointTests() = default;

    ~DBCheckpointTests() override = default;
};

TEST_F(DBCheckpointTests, CheckpointerBlocksOtherCheckpointers)
{
    ASSERT_OK(try_reopen(true));
    m_test_env->add_interceptor(
        kDBName,
        tools::Interceptor(tools::kSyscallWrite, [this] {
            // Each time File::write() is called, use a different connection to attempt a
            // checkpoint. It should get blocked every time, since a checkpoint is already
            // running.
            DB *db;
            Options options;
            options.env = m_test_env;
            EXPECT_OK(DB::open(options, kDBName, db));
            EXPECT_TRUE(db->checkpoint(false).is_busy());
            EXPECT_TRUE(db->checkpoint(true).is_busy());
            delete db;
            return Status::ok();
        }));
    ASSERT_OK(m_db->checkpoint(true));
}

TEST_F(DBCheckpointTests, CheckpointerAllowsTransactions)
{
    static constexpr std::size_t kCkptCount = 1'000;

    // Set up a DB with some records in both the database file and the WAL.
    ASSERT_OK(try_reopen(true));
    ASSERT_OK(m_db->checkpoint(true));
    ASSERT_OK(m_db->update([](auto &txn) {
        // These records will be checkpointed below. `round` is 1 to cause a new version of the first half of
        // the records to be written.
        return put_range(txn, TableOptions(), "saved", 0, kSavedCount / 2, 1);
    }));

    U64 n = 0;
    m_test_env->add_interceptor(
        kDBName,
        tools::Interceptor(tools::kSyscallWrite, [this, &n] {
            DB *db;
            Options options;
            options.env = m_test_env;
            CHECK_OK(DB::open(options, kDBName, db));
            EXPECT_OK(db->update([n](auto &txn) {
                return put_range(txn, TableOptions(), "SELF", n * 2, (n + 1) * 2);
            }));
            (void)db->view([n](auto &txn) {
                // The version 0 records must come from the database file.
                EXPECT_OK(check_range(txn, TableOptions(), "saved", 0, kSavedCount / 2, true, 0));
                // The version 1 records must come from the WAL.
                EXPECT_OK(check_range(txn, TableOptions(), "saved", kSavedCount / 2, kSavedCount, true, 1));
                EXPECT_OK(check_range(txn, TableOptions(), "SELF", 0, (n + 1) * 2, true));
                return Status::ok();
            });
            ++n;
            delete db;
            return Status::ok();
        }));
    ASSERT_OK(m_db->checkpoint(false));
}

} // namespace calicodb
