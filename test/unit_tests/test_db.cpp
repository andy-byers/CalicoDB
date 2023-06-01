// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "db_impl.h"
#include "freelist.h"
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
        // 3 pages is long enough to generate both types of overflow pages (kOverflowHead
        // and kOverflowLink).
        static constexpr std::size_t kMaxKV = kPageSize * 3;
        const auto key_length = (round + 1) * kMaxKV / kMaxRounds;
        auto key_str = tools::integral_key(kv);
        const auto val_length = kMaxKV - key_length;
        auto val_str = number_to_string(kv);
        if (val_str.size() < val_length) {
            val_str.resize(kPageSize / 4 - val_str.size(), '0');
        }
        return {key_str, val_str};
    }

    [[nodiscard]] static auto put(Tx &tx, const Bucket &b, int kv, int round = 0) -> Status
    {
        const auto [k, v] = make_kv(kv, round);
        return tx.put(b, k, v);
    }
    [[nodiscard]] static auto put(Tx &tx, const BucketOptions &options, const std::string &bname, int kv, int round = 0) -> Status
    {
        Bucket b;
        auto s = tx.create_bucket(options, bname, &b);
        if (s.is_ok()) {
            s = put(tx, b, kv, round);
        }
        return s;
    }

    [[nodiscard]] static auto put_range(Tx &tx, const Bucket &b, int kv1, int kv2, int round = 0) -> Status
    {
        Status s;
        for (int kv = kv1; s.is_ok() && kv < kv2; ++kv) {
            s = put(tx, b, kv, round);
        }
        return s;
    }
    [[nodiscard]] static auto put_range(Tx &tx, const BucketOptions &options, const std::string &bname, int kv1, int kv2, int round = 0) -> Status
    {
        Bucket b;
        auto s = tx.create_bucket(options, bname, &b);
        if (s.is_ok()) {
            s = put_range(tx, b, kv1, kv2, round);
        }
        return s;
    }

    [[nodiscard]] static auto erase(Tx &tx, const Bucket &b, int kv, int round = 0) -> Status
    {
        const auto [k, _] = make_kv(kv, round);
        return tx.erase(b, k);
    }
    [[nodiscard]] static auto erase(Tx &tx, const BucketOptions &options, const std::string &bname, int kv, int round = 0) -> Status
    {
        Bucket b;
        auto s = tx.create_bucket(options, bname, &b);
        if (s.is_ok()) {
            s = erase(tx, b, kv, round);
        }
        return s;
    }

    [[nodiscard]] static auto erase_range(Tx &tx, const Bucket &b, int kv1, int kv2, int round = 0) -> Status
    {
        Status s;
        for (int kv = kv1; s.is_ok() && kv < kv2; ++kv) {
            s = erase(tx, b, kv, round);
        }
        return s;
    }
    [[nodiscard]] static auto erase_range(Tx &tx, const BucketOptions &options, const std::string &bname, int kv1, int kv2, int round = 0) -> Status
    {
        Bucket b;
        auto s = tx.create_bucket(options, bname, &b);
        if (s.is_ok()) {
            s = erase_range(tx, b, kv1, kv2, round);
        }
        return s;
    }

    [[nodiscard]] static auto check(Tx &tx, const Bucket &b, int kv, bool exists, int round = 0) -> Status
    {
        std::string result;
        const auto [k, v] = make_kv(kv, round);
        auto s = tx.get(b, k, &result);
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
    [[nodiscard]] static auto check(Tx &tx, const BucketOptions &options, const std::string &bname, int kv, bool exists, int round = 0) -> Status
    {
        Bucket b;
        auto s = tx.create_bucket(options, bname, &b);
        if (s.is_ok()) {
            s = check(tx, b, kv, exists, round);
        }
        return s;
    }

    [[nodiscard]] static auto check_range(const Tx &tx, const Bucket &b, int kv1, int kv2, bool exists, int round = 0) -> Status
    {
        auto *c = tx.new_cursor(b);
        // Run some extra seek*() calls.
        if (kv1 & 1) {
            c->seek_first();
        } else {
            c->seek_last();
        }
        Status s;
        if (c->status().is_io_error()) {
            s = c->status();
        }
        if (s.is_ok() && exists) {
            for (int kv = kv1; kv < kv2; ++kv) {
                const auto [k, v] = make_kv(kv, round);
                if (kv == kv1) {
                    c->seek(k);
                }
                if (c->is_valid()) {
                    EXPECT_EQ(k, c->key().to_string());
                    EXPECT_EQ(v, c->value().to_string());
                } else {
                    EXPECT_TRUE(c->status().is_io_error());
                    s = c->status();
                    break;
                }
                c->next();
            }
            if (s.is_ok()) {
                for (int kv = kv2 - 1; kv >= kv1; --kv) {
                    const auto [k, v] = make_kv(kv, round);
                    if (kv == kv2 - 1) {
                        c->seek(k);
                    }
                    if (c->is_valid()) {
                        EXPECT_EQ(k, c->key());
                        EXPECT_EQ(v, c->value());
                    } else {
                        s = c->status();
                        break;
                    }
                    c->previous();
                }
            }
        } else {
            for (int kv = kv1; kv < kv2; ++kv) {
                const auto [k, v] = make_kv(kv, round);
                c->seek(k);
                if (c->is_valid()) {
                    EXPECT_NE(k, c->key().to_string());
                } else if (!c->status().is_ok()) {
                    EXPECT_TRUE((c->status().is_io_error()));
                    s = c->status();
                    break;
                }
                ++kv;
            }
        }
        delete c;
        return s;
    }
    [[nodiscard]] static auto check_range(const Tx &tx, const std::string &bname, int kv1, int kv2, bool exists, int round = 0) -> Status
    {
        Bucket b;
        auto s = tx.open_bucket(bname, b);
        if (s.is_ok()) {
            s = check_range(tx, b, kv1, kv2, exists, round);
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
                break;
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
        return m_config <= kMaxConfig;
    }

    [[nodiscard]] auto file_size(const std::string &filename) const -> std::size_t
    {
        std::size_t file_size;
        EXPECT_OK(m_env->file_size(filename, file_size));
        return file_size;
    }

    static constexpr std::size_t kMaxBuckets = 13;
    static constexpr const char kBucketStr[kMaxBuckets + 2] = "BUCKET_NAMING_";
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
    db_impl(const_db);
    ASSERT_OK(m_db->update([](auto &tx) {
        const auto &const_tx = tx;
        tx_impl(&tx)->TEST_validate();
        tx_impl(&const_tx)->TEST_validate();
        return Status::ok();
    }));
}

TEST_F(DBTests, NewTx)
{
    const Tx *reader1, *reader2 = reinterpret_cast<const Tx *>(42);
    Tx *writer1, *writer2 = reinterpret_cast<Tx *>(42);

    ASSERT_OK(m_db->new_tx(WriteTag{}, writer1));
    ASSERT_NE(nullptr, writer2);
    ASSERT_NOK(m_db->new_tx(WriteTag{}, writer2));
    ASSERT_EQ(nullptr, writer2);
    delete writer1;

    ASSERT_OK(m_db->new_tx(WriteTag{}, writer2));
    ASSERT_NE(nullptr, reader2);
    ASSERT_NOK(m_db->new_tx(reader2));
    ASSERT_EQ(nullptr, reader2);
    delete writer2;

    ASSERT_OK(m_db->new_tx(reader2));
    ASSERT_NE(nullptr, writer2);
    ASSERT_NOK(m_db->new_tx(WriteTag{}, writer2));
    ASSERT_EQ(nullptr, writer2);
    delete reader2;

    ASSERT_OK(m_db->new_tx(reader1));
    ASSERT_NE(nullptr, reader2);
    ASSERT_NOK(m_db->new_tx(reader2));
    ASSERT_EQ(nullptr, reader2);
    delete reader1;

    std::vector<std::string> values;
    auto s = m_db->view([&values](const auto &tx) {
        Bucket b;
        auto s = tx.open_bucket("bucket", b);
        if (s.is_ok()) {
            auto *c = tx.new_cursor(b);
            c->seek_first();
            while (c->is_valid()) {
                if (c->key().starts_with("common-prefix")) {
                    values.emplace_back(c->value().to_string());
                }
                c->next();
            }
            s = c->status();
            delete c;
        }
        return s;
    });
}

TEST_F(DBTests, NewBucket)
{
    ASSERT_OK(m_db->update([](auto &tx) {
        Bucket b;
        BucketOptions tbopt;
        EXPECT_NOK(tx.open_bucket("BUCKET", b));
        EXPECT_OK(tx.create_bucket(tbopt, "BUCKET", &b));
        return Status::ok();
    }));
}

TEST_F(DBTests, BucketBehavior)
{
    ASSERT_OK(m_db->update([](auto &tx) {
        Bucket b;
        EXPECT_OK(tx.create_bucket(BucketOptions(), "BUCKET", &b));
        // Tx::put() should not accept an empty key.
        EXPECT_TRUE(tx.put(b, "", "value").is_invalid_argument());
        return Status::ok();
    }));
}

TEST_F(DBTests, ReadonlyTx)
{
    ASSERT_OK(m_db->update([](auto &tx) {
        Bucket b;
        EXPECT_OK(tx.create_bucket(BucketOptions(), "BUCKET", &b));
        return Status::ok();
    }));
    ASSERT_OK(m_db->view([](auto &tx) {
        Bucket b;
        EXPECT_OK(tx.open_bucket("BUCKET", b));
        auto *c = tx.new_cursor(b);
        delete c;
        c = &tx.schema();
        return Status::ok();
    }));
}

TEST_F(DBTests, UpdateThenView)
{
    int round = 0;
    do {
        BucketOptions tbopt;
        tbopt.error_if_exists = true;
        for (int i = 0; i < 3; ++i) {
            ASSERT_OK(m_db->update([i, tbopt, round](auto &tx) {
                Bucket b;
                auto s = tx.create_bucket(tbopt, kBucketStr + i, &b);
                if (s.is_ok()) {
                    s = put_range(tx, b, 0, 1'000, round);
                    if (s.is_ok()) {
                        s = erase_range(tx, b, 250, 750, round);
                    }
                }
                return s;
            }));
        }
        for (int i = 0; i < 3; ++i) {
            ASSERT_OK(m_db->view([round, i](auto &tx) {
                Bucket b;
                auto s = tx.open_bucket(kBucketStr + i, b);
                if (s.is_ok()) {
                    EXPECT_OK(check_range(tx, b, 0, 250, true, round));
                    EXPECT_OK(check_range(tx, b, 250, 750, false, round));
                    EXPECT_OK(check_range(tx, b, 750, 1'000, true, round));
                }
                return s;
            }));
        }
        ASSERT_OK(m_db->update([](auto &tx) {
            return tx.vacuum();
        }));
        ASSERT_OK(m_db->checkpoint(false));
        ++round;
    } while (change_options(true));
}

TEST_F(DBTests, RollbackUpdate)
{
    int round = 0;
    do {
        for (int i = 0; i < 3; ++i) {
            ASSERT_TRUE(m_db->update([i, round](auto &tx) {
                                Bucket b;
                                auto s = tx.create_bucket(BucketOptions(), kBucketStr + i, &b);
                                if (s.is_ok()) {
                                    s = put_range(tx, b, 0, 500, round);
                                    if (s.is_ok()) {
                                        // We have access to the Tx here, so we can actually call
                                        // Tx::commit() as many times as we want before we return.
                                        // The returned status determines whether to perform a final
                                        // commit before calling delete on the Tx.
                                        s = tx.commit();
                                        if (s.is_ok()) {
                                            s = put_range(tx, b, 500, 1'000, round);
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
            ASSERT_OK(m_db->view([round, i](const auto &tx) {
                Bucket b;
                auto s = tx.open_bucket(kBucketStr + i, b);
                if (s.is_ok()) {
                    EXPECT_OK(check_range(tx, b, 0, 500, true, round));
                    EXPECT_OK(check_range(tx, b, 500, 1'000, false, round));
                }
                return s;
            }));
        }
        ASSERT_OK(m_db->checkpoint(false));
        ++round;
    } while (change_options(true));
}

TEST_F(DBTests, VacuumEmptyDB)
{
    ASSERT_OK(m_db->update([](auto &tx) {
        return tx.vacuum();
    }));
}

TEST_F(DBTests, CorruptedRootIDs)
{
    ASSERT_OK(m_db->update([](auto &tx) {
        Bucket *tb1, *tb2;
        EXPECT_OK(put_range(tx, BucketOptions(), "BUCKET", 0, 10));
        EXPECT_OK(put_range(tx, BucketOptions(), "temp", 0, 10));
        return tx.drop_bucket("temp");
    }));
    ASSERT_OK(m_db->checkpoint(true));

    File *file;
    auto *env = Env::default_env();
    ASSERT_OK(env->new_file(kDBName, Env::kReadWrite, file));

    // Corrupt the root ID written to the schema bucket, which has already been
    // written back to the database file. The root ID is a 1 byte varint pointing
    // to page 3. Just increment it, which makes a root that points past the end
    // of the file, which is not allowed.
    char buffer[kPageSize];
    ASSERT_OK(file->read_exact(0, sizeof(buffer), buffer));
    buffer[kPageSize - 1] = 42; // Corrupt the root ID of "BUCKET".
    ASSERT_OK(file->write(0, Slice(buffer, kPageSize)));
    delete file;

    (void)m_db->update([](auto &tx) {
        Status s;
        EXPECT_TRUE((s = tx.create_bucket(BucketOptions(), "BUCKET", nullptr)).is_corruption())
            << s.to_string();
        // The corrupted root ID cannot be fixed by this rollback. The corruption
        // happened outside of a transaction. Future transactions should also see
        // the corrupted root and fail.
        return s;
    });
    (void)m_db->update([](auto &tx) {
        Status s;
        EXPECT_TRUE((s = tx.drop_bucket("BUCKET")).is_corruption())
            << s.to_string();
        return s;
    });
    (void)m_db->update([](auto &tx) {
        Status s;
        EXPECT_TRUE((s = tx.vacuum()).is_corruption())
            << s.to_string();
        return s;
    });
}

TEST_F(DBTests, CheckpointResize)
{
    ASSERT_OK(m_db->update([](auto &tx) {
        Bucket b;
        auto s = tx.create_bucket(BucketOptions(), "BUCKET", &b);
        if (s.is_ok()) {
        }
        return s;
    }));
    ASSERT_EQ(0, file_size(kDBName));

    ASSERT_OK(m_db->checkpoint(true));
    ASSERT_EQ(kPageSize * 3, file_size(kDBName));

    ASSERT_OK(m_db->update([](auto &tx) {
        auto s = tx.drop_bucket("BUCKET");
        if (s.is_ok()) {
            s = tx.vacuum();
        }
        return s;
    }));
    ASSERT_EQ(kPageSize * 3, file_size(kDBName));

    // Tx::vacuum() never gets rid of the root database page, even if the whole database
    // is empty.
    ASSERT_OK(m_db->checkpoint(true));
    ASSERT_EQ(kPageSize, file_size(kDBName));
}

TEST_F(DBTests, RerootBuckets)
{
    ASSERT_OK(m_db->update([](auto &tx) {
        EXPECT_OK(tx.create_bucket(BucketOptions(), "a", nullptr));
        EXPECT_OK(tx.create_bucket(BucketOptions(), "b", nullptr));
        EXPECT_OK(tx.create_bucket(BucketOptions(), "c", nullptr));
        EXPECT_OK(tx.create_bucket(BucketOptions(), "d", nullptr));
        tx_impl(&tx)->TEST_validate();
        EXPECT_OK(tx.drop_bucket("a"));
        EXPECT_OK(tx.drop_bucket("b"));
        EXPECT_OK(tx.drop_bucket("d"));
        return Status::ok();
    }));
    ASSERT_OK(m_db->update([](auto &tx) {
        EXPECT_OK(tx.create_bucket(BucketOptions(), "e", nullptr));
        return tx.vacuum();
    }));
    ASSERT_OK(m_db->view([](auto &tx) {
        Bucket c, e;
        auto &schema = tx.schema();
        schema.seek_first();
        EXPECT_TRUE(schema.is_valid());
        EXPECT_EQ("c", schema.key());
        EXPECT_OK(tx.open_bucket(schema.key().to_string(), c));
        schema.next();
        EXPECT_TRUE(schema.is_valid());
        EXPECT_EQ("e", schema.key());
        EXPECT_OK(tx.open_bucket(schema.key().to_string(), e));
        schema.previous();
        EXPECT_TRUE(schema.is_valid());
        schema.next();
        schema.next();
        EXPECT_FALSE(schema.is_valid());
        return Status::ok();
    }));
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

//TEST(DestructionTests, OnlyDeletesCalicoDatabases)
//{
//    std::filesystem::remove_all("./testdb");
//
//    // "./testdb" does not exist.
//    ASSERT_TRUE(DB::destroy(Options(), "./testdb").is_invalid_argument());
//    ASSERT_FALSE(Env::default_env()->file_exists("./testdb"));
//
//    // File is too small to read the first page.
//    File *file;
//    ASSERT_OK(Env::default_env()->new_file("./testdb", Env::kCreate, file));
//    ASSERT_OK(file->write(0, "CalicoDB format"));
//    ASSERT_TRUE(DB::destroy(Options(), "./testdb").is_invalid_argument());
//    ASSERT_TRUE(Env::default_env()->file_exists("./testdb"));
//
//    // Identifier is incorrect.
//    ASSERT_OK(file->write(0, "CalicoDB format 0"));
//    ASSERT_TRUE(DB::destroy(Options(), "./testdb").is_invalid_argument());
//
//    DB *db;
//    std::filesystem::remove_all("./testdb");
//    ASSERT_OK(DB::open(Options(), "./testdb", db));
//    ASSERT_OK(DB::destroy(Options(), "./testdb"));
//
//    delete db;
//    delete file;
//}

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

    using OpenFlag = unsigned;
    static constexpr OpenFlag kPrefill = 1;
    static constexpr OpenFlag kKeepOpen = 2;
    static constexpr OpenFlag kClearDB = 4;
    [[nodiscard]] auto try_reopen(OpenFlag flag = 0) -> Status
    {
        Status s;
        if (0 == (flag & kKeepOpen)) {
            m_config = kSyncMode;
            s = reopen_db(flag & kClearDB, m_test_env);
        }
        if (s.is_ok() && (flag & kPrefill) && m_max_count == 0) {
            // The first time the DB is opened, add kSavedCount records to the WAL and
            // commit.
            s = m_db->update([](auto &tx) {
                return put_range(tx, BucketOptions(), "saved", 0, kSavedCount);
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
    ASSERT_OK(try_reopen(kPrefill));
    set_error(tools::kSyscallRead);

    for (;;) {
        auto s = m_db->view([](auto &tx) {
            Bucket b;
            auto s = tx.open_bucket("saved", b);
            if (s.is_ok()) {
                s = check_range(tx, b, 0, kSavedCount, true);
                if (s.is_ok()) {
                    s = check_range(tx, b, kSavedCount, 2 * kSavedCount, false);
                }
            }
            EXPECT_OK(tx.status());
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
    ASSERT_OK(try_reopen(kPrefill));
    set_error(tools::kSyscallWrite | tools::kSyscallSync);

    for (;;) {
        auto s = try_reopen();
        if (s.is_ok()) {
            s = m_db->update([](auto &tx) {
                Bucket b;
                std::string op("create_bucket()");
                auto s = tx.create_bucket(BucketOptions(), "BUCKET", &b);
                if (s.is_ok()) {
                    op = "put_range()";
                    s = put_range(tx, b, 0, 1'000);
                    if (!s.is_ok()) {
                        auto *c = tx.new_cursor(b);
                        EXPECT_EQ(s, c->status());
                        delete c;
                    }
                }
                EXPECT_EQ(s, tx.status()) << "status mismatch:\n  \"" << s.to_string()
                                          << "\"\n  \"" << tx.status().to_string() << "\"\n"
                                          << "during " << op << '\n';
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
    ASSERT_OK(try_reopen());
    ASSERT_OK(m_db->view([](const auto &tx) {
        return check_range(tx, "BUCKET", 0, kSavedCount, true);
    }));
    ASSERT_LT(0, m_max_count);
}

TEST_F(DBErrorTests, Checkpoint)
{
    // Add some records to the WAL and set the next syscall to fail. The checkpoint during
    // the close routine will fail.
    ASSERT_OK(try_reopen(kPrefill));
    set_error(kAllSyscalls);

    for (Status s;;) {
        s = try_reopen();
        if (s.is_ok()) {
            s = m_db->checkpoint(true);
        }
        if (s.is_ok()) {
            m_test_env->clear_interceptors();
            break;
        }
        ASSERT_EQ(kErrorMessage, s.to_string());
        reset_error();
    }

    ASSERT_OK(try_reopen());
    ASSERT_OK(m_db->view([](const auto &tx) {
        return check_range(tx, "saved", 0, kSavedCount, true);
    }));
    ASSERT_LT(0, m_max_count);
}

TEST_F(DBErrorTests, TransactionsAfterCheckpointFailure)
{
    const auto check_db = [](const auto &tx) {
        // These records are in the database file.
        auto s = check_range(tx, "saved", 0, kSavedCount, true);
        if (s.is_ok()) {
            // These records are in the WAL (and maybe partially written back to the database file).
            s = check_range(tx, "pending", 0, kSavedCount, true);
            if (s.is_ok()) {
                // These records were written after the failed checkpoint.
                s = check_range(tx, "after", 0, kSavedCount, true);
            }
        }
        return s;
    };

    // Create a situation where we need to look in the database file for some records
    // and the WAL file for others.
    ASSERT_OK(try_reopen(kPrefill));
    ASSERT_OK(m_db->checkpoint(true));
    ASSERT_OK(m_db->update([](auto &tx) {
        return put_range(tx, BucketOptions(), "pending", 0, kSavedCount);
    }));
    set_error(kAllSyscalls);

    for (Status s;;) {
        s = try_reopen(kKeepOpen);
        if (s.is_ok()) {
            s = m_db->checkpoint(true);
        }
        if (!s.is_ok()) {
            ASSERT_EQ(kErrorMessage, s.to_string());
            if (m_db) {
                // Stop generating faults.
                m_counter = -1;
                ASSERT_OK(m_db->update([](auto &tx) {
                    Bucket b;
                    BucketOptions bopt;
                    bopt.error_if_exists = true;
                    auto s = tx.create_bucket(bopt, "after", &b);
                    if (s.is_ok()) {
                        s = put_range(tx, BucketOptions(), "after", 0, kSavedCount);
                    } else if (s.is_invalid_argument()) {
                        s = Status::ok();
                    }
                    return s;
                }));
                ASSERT_OK(m_db->view(check_db));
            }
        } else {
            m_test_env->clear_interceptors();
            break;
        }
        reset_error();
    }
    ASSERT_OK(reopen_db(false));
    ASSERT_OK(m_db->view(check_db));
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
    // 1. If the bucket named "BUCKET" exists, it contains kRecordCount records
    // 2. Record keys are monotonically increasing integers starting from 0, serialized
    //    using tools::integral_key()
    // 3. Each record value is another such serialized integer, however, each value is
    //    identical
    // 4. The record value read by a reader must never decrease between runs
    [[nodiscard]] static auto reader(DB &db, U64 &latest) -> Status
    {
        return db.view([&latest](const auto &tx) {
            Bucket b;
            auto s = tx.open_bucket("BUCKET", b);
            if (s.is_invalid_argument()) {
                // Writer hasn't created the bucket yet.
                return Status::ok();
            } else if (!s.is_ok()) {
                return s;
            }
            // Iterate through the records twice. The same value should be read each time.
            for (std::size_t i = 0; i < kRecordCount * 2; ++i) {
                std::string value;
                // If the bucket exists, then it must contain kRecordCount records (the first writer to run
                // makes sure of this).
                s = tx.get(b, tools::integral_key(i % kRecordCount), &value);
                if (s.is_ok()) {
                    U64 result;
                    Slice slice(value);
                    EXPECT_TRUE(consume_decimal_number(slice, &result));
                    if (i) {
                        EXPECT_EQ(latest, result);
                    } else {
                        EXPECT_LE(latest, result);
                        latest = result;
                    }
                } else {
                    break;
                }
            }
            return s;
        });
    }

    // Writer tasks set up invariants on the DB for the reader to check. Each writer
    // either creates or increases kRecordCount records in a bucket named "BUCKET". The
    // first writer to run creates the bucket.
    [[nodiscard]] static auto writer(DB &db) -> Status
    {
        return db.update([](auto &tx) {
            Bucket b;
            auto s = tx.create_bucket(BucketOptions(), "BUCKET", &b);
            for (std::size_t i = 0; s.is_ok() && i < kRecordCount; ++i) {
                U64 result = 1;
                std::string value;
                s = tx.get(b, tools::integral_key(i), &value);
                if (s.is_not_found()) {
                    s = Status::ok();
                } else if (s.is_ok()) {
                    Slice slice(value);
                    EXPECT_TRUE(consume_decimal_number(slice, &result));
                    ++result;
                } else {
                    break;
                }
                s = tx.put(b, tools::integral_key(i), tools::integral_key(result));
            }
            EXPECT_OK(s);
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

                count.fetch_add(1);
                while (count.load() < total) {
                    std::this_thread::yield();
                }

                Status s;
                if (i < nrd) {
                    // Readers should never block. Anything that would block a reader resolves in a
                    // bounded amount of time, so the implementation just waits.
                    ASSERT_OK(reader(*db, latest[i])) << "reader (" << i << ") failed";
                } else if (i < nrd + nwr) {
                    while ((s = writer(*db)).is_busy()) {
                    }
                    ASSERT_OK(s) << "writer (" << i << ") failed";
                } else {
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
    ASSERT_OK(try_reopen(kPrefill));
    const auto intercept = [this, &key_limit, &should_exist] {
        DB *db;
        Options options;
        options.env = m_test_env;
        EXPECT_OK(DB::open(options, kDBName, db));
        auto s = db->view([key_limit](auto &tx) {
            return check_range(tx, "BUCKET", 0, key_limit * 10, true);
        });
        if (!should_exist && s.is_invalid_argument()) {
            s = Status::ok();
        }
        delete db;
        return s;
    };
    m_test_env->add_interceptor(kWALName, tools::Interceptor(tools::kSyscallWrite, intercept));
    (void)m_db->update([&should_exist, &key_limit](auto &tx) {
        for (std::size_t i = 0; i < 50; ++i) {
            EXPECT_OK(put_range(tx, BucketOptions(), "BUCKET", i * 10, (i + 1) * 10));
            EXPECT_OK(tx.commit());
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

    ASSERT_OK(try_reopen(kPrefill));
    const auto intercept = [this, &has_open_db, &n] {
        if (has_open_db || n >= kN) {
            // Prevent this callback from being called by itself, and prevent the test from
            // running for too long.
            return Status::ok();
        }
        DB *db;
        Options options;
        options.env = m_test_env;
        has_open_db = true;
        EXPECT_OK(DB::open(options, kDBName, db));
        EXPECT_OK(db->update([n](auto &tx) {
            return put_range(tx, BucketOptions(), "BUCKET", kN * n, kN * (n + 1));
        }));
        delete db;
        has_open_db = false;
        ++n;
        return Status::ok();
    };
    ASSERT_OK(m_db->update([](auto &tx) {
        return put_range(tx, BucketOptions(), "BUCKET", 0, kN);
    }));
    m_test_env->add_interceptor(kWALName, tools::Interceptor(tools::kSyscallRead, intercept));
    (void)m_db->view([&n](auto &tx) {
        for (std::size_t i = 0; i < kN; ++i) {
            EXPECT_OK(check_range(tx, "BUCKET", 0, kN, true));
            EXPECT_OK(check_range(tx, "BUCKET", kN, kN * (n + 1), false));
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
    ASSERT_OK(try_reopen(kPrefill));
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
    ASSERT_OK(try_reopen(kPrefill));
    ASSERT_OK(m_db->checkpoint(true));
    ASSERT_OK(m_db->update([](auto &tx) {
        // These records will be checkpointed below. `round` is 1 to cause a new version of the first half of
        // the records to be written.
        return put_range(tx, BucketOptions(), "saved", 0, kSavedCount / 2, 1);
    }));

    U64 n = 0;
    m_test_env->add_interceptor(
        kDBName,
        tools::Interceptor(tools::kSyscallWrite, [this, &n] {
            DB *db;
            Options options;
            options.env = m_test_env;
            CHECK_OK(DB::open(options, kDBName, db));
            EXPECT_OK(db->update([n](auto &tx) {
                return put_range(tx, BucketOptions(), "SELF", n * 2, (n + 1) * 2);
            }));
            (void)db->view([n](auto &tx) {
                // The version 0 records must come from the database file.
                EXPECT_OK(check_range(tx, "saved", 0, kSavedCount / 2, true, 0));
                // The version 1 records must come from the WAL.
                EXPECT_OK(check_range(tx, "saved", kSavedCount / 2, kSavedCount, true, 1));
                EXPECT_OK(check_range(tx, "SELF", 0, (n + 1) * 2, true));
                return Status::ok();
            });
            ++n;
            delete db;
            return Status::ok();
        }));
    ASSERT_OK(m_db->checkpoint(false));
}

class DBVacuumTests : public DBTests
{
protected:
    explicit DBVacuumTests() = default;

    ~DBVacuumTests() override = default;

    auto test_configurations_impl(const std::vector<U8> &bitmaps) const -> void
    {
        static constexpr auto *kName = "12345678_BUCKET_NAMES";
        static constexpr std::size_t kN = 10;
        (void)m_db->update([&bitmaps](auto &tx) {
            Bucket buckets[8];
            for (std::size_t i = 0; i < 8; ++i) {
                EXPECT_OK(tx.create_bucket(BucketOptions(), kName + i, &buckets[i]));
            }
            std::vector<std::size_t> bs;
            std::vector<std::size_t> is;
            for (std::size_t b = 0; b < bitmaps.size(); ++b) {
                for (std::size_t i = 0; i < 8; ++i) {
                    if ((bitmaps[b] >> i) & 1) {
                        EXPECT_OK(put_range(tx, buckets[i], b * kN, (b + 1) * kN));
                        bs.emplace_back(b);
                        is.emplace_back(i);
                    }
                }
            }
            for (std::size_t n = 0; n < bs.size(); ++n) {
                if (0 == (n & 1)) {
                    EXPECT_OK(erase_range(tx, buckets[is[n]], bs[n] * kN, (bs[n] + 1) * kN));
                }
            }
            EXPECT_OK(tx.vacuum());

            for (std::size_t n = 0; n < bs.size(); ++n) {
                EXPECT_OK(check_range(tx, buckets[is[n]], bs[n] * kN, (bs[n] + 1) * kN, n & 1));
                if (n & 1) {
                    // Erase the rest of the records. The database should be empty after this
                    // loop completes.
                    EXPECT_OK(erase_range(tx, buckets[is[n]], bs[n] * kN, (bs[n] + 1) * kN));
                }
            }
            EXPECT_OK(tx.vacuum());

            for (std::size_t n = 0; n < bs.size(); ++n) {
                EXPECT_OK(check_range(tx, buckets[is[n]], bs[n] * kN, (bs[n] + 1) * kN, false));
            }
            return Status::ok();
        });
    }
    auto test_configurations(std::vector<U8> bitmaps) const -> void
    {
        for (U32 i = 0; i < 8; ++i) {
            for (auto &b : bitmaps) {
                b = (b << 1) | (b >> 7);
            }
            test_configurations_impl(bitmaps);
        }
    }
};

TEST_F(DBVacuumTests, SingleBucket)
{
    test_configurations({
        0b10000000,
        0b10000000,
        0b10000000,
        0b10000000,
    });
}

TEST_F(DBVacuumTests, MultipleBuckets)
{
    test_configurations({
        0b10000000,
        0b01000000,
        0b00100000,
        0b00010000,
    });
    test_configurations({
        0b10001000,
        0b01000100,
        0b00100010,
        0b00010001,
    });
    test_configurations({
        0b10101000,
        0b01010100,
        0b00101010,
        0b00010101,
    });
    test_configurations({
        0b10101010,
        0b01010101,
        0b10101010,
        0b01010101,
    });
}

TEST_F(DBVacuumTests, SanityCheck)
{
    test_configurations({
        0b11111111,
        0b11111111,
        0b11111111,
        0b11111111,
    });
}

} // namespace calicodb
