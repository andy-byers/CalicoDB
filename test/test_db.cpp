// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "common.h"
#include "db_impl.h"
#include "fake_env.h"
#include "header.h"
#include "logging.h"
#include "test.h"
#include "tx_impl.h"
#include <gtest/gtest.h>

namespace calicodb::test
{

TEST(FileFormatTests, ReportsUnrecognizedFormatString)
{
    char page[kPageSize];
    FileHdr::make_supported_db(page);

    ++page[0];
    ASSERT_NOK(FileHdr::check_db_support(page));
}

TEST(FileFormatTests, ReportsUnrecognizedFormatVersion)
{
    char page[kPageSize];
    FileHdr::make_supported_db(page);

    ++page[FileHdr::kFmtVersionOffset];
    ASSERT_NOK(FileHdr::check_db_support(page));
}

class DBTests : public testing::Test
{
protected:
    static constexpr std::size_t kMaxRounds = 1'000;
    const std::string m_test_dir;
    const std::string m_db_name;
    const std::string m_alt_wal_name;

    explicit DBTests()
        : m_test_dir(testing::TempDir()),
          m_db_name(m_test_dir + "db"),
          m_alt_wal_name(m_test_dir + "wal"),
          m_env(Env::default_env())
    {
        (void)DB::destroy(Options(), m_db_name);
    }

    ~DBTests() override
    {
        delete m_db;
    }

    auto SetUp() -> void override
    {
        ASSERT_OK(reopen_db(false));
    }

    [[nodiscard]] static auto make_kv(std::size_t kv, std::size_t round = 0) -> std::pair<std::string, std::string>
    {
        EXPECT_LE(0, kv);
        EXPECT_LE(0, round);
        // 3 pages is long enough to generate both types of overflow pages (kOverflowHead
        // and kOverflowLink).
        static constexpr std::size_t kMaxKV = kPageSize * 3;
        const auto key_length = (round + 1) * kMaxKV / kMaxRounds;
        auto key_str = numeric_key<kMaxKV>(kv);
        key_str = key_str.substr(kMaxKV - key_length);
        const auto val_length = kMaxKV - key_length;
        auto val_str = number_to_string(kv);
        if (val_str.size() < val_length) {
            val_str.resize(kPageSize / 4 - val_str.size(), '0');
        }
        return {key_str, val_str};
    }

    [[nodiscard]] static auto put(Tx &tx, const Bucket &b, std::size_t kv, std::size_t round = 0) -> Status
    {
        const auto [k, v] = make_kv(kv, round);
        return tx.put(b, k, v);
    }
    [[nodiscard]] static auto put(Tx &tx, const BucketOptions &options, const std::string &bname, std::size_t kv, std::size_t round = 0) -> Status
    {
        Bucket b;
        auto s = tx.create_bucket(options, bname, &b);
        if (s.is_ok()) {
            s = put(tx, b, kv, round);
        }
        return s;
    }

    [[nodiscard]] static auto put_range(Tx &tx, const Bucket &b, std::size_t kv1, std::size_t kv2, std::size_t round = 0) -> Status
    {
        Status s;
        for (std::size_t kv = kv1; s.is_ok() && kv < kv2; ++kv) {
            s = put(tx, b, kv, round);
        }
        return s;
    }
    [[nodiscard]] static auto put_range(Tx &tx, const BucketOptions &options, const std::string &bname, std::size_t kv1, std::size_t kv2, std::size_t round = 0) -> Status
    {
        Bucket b;
        auto s = tx.create_bucket(options, bname, &b);
        if (s.is_ok()) {
            s = put_range(tx, b, kv1, kv2, round);
        }
        return s;
    }

    [[nodiscard]] static auto erase(Tx &tx, const Bucket &b, std::size_t kv, std::size_t round = 0) -> Status
    {
        const auto [k, _] = make_kv(kv, round);
        return tx.erase(b, k);
    }
    [[nodiscard]] static auto erase(Tx &tx, const BucketOptions &options, const std::string &bname, std::size_t kv, std::size_t round = 0) -> Status
    {
        Bucket b;
        auto s = tx.create_bucket(options, bname, &b);
        if (s.is_ok()) {
            s = erase(tx, b, kv, round);
        }
        return s;
    }

    [[nodiscard]] static auto erase_range(Tx &tx, const Bucket &b, std::size_t kv1, std::size_t kv2, std::size_t round = 0) -> Status
    {
        Status s;
        for (std::size_t kv = kv1; s.is_ok() && kv < kv2; ++kv) {
            s = erase(tx, b, kv, round);
        }
        return s;
    }
    [[nodiscard]] static auto erase_range(Tx &tx, const BucketOptions &options, const std::string &bname, std::size_t kv1, std::size_t kv2, std::size_t round = 0) -> Status
    {
        Bucket b;
        auto s = tx.create_bucket(options, bname, &b);
        if (s.is_ok()) {
            s = erase_range(tx, b, kv1, kv2, round);
        }
        return s;
    }

    [[nodiscard]] static auto check(Tx &tx, const Bucket &b, std::size_t kv, bool exists, std::size_t round = 0) -> Status
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
    [[nodiscard]] static auto check(Tx &tx, const BucketOptions &options, const std::string &bname, std::size_t kv, bool exists, std::size_t round = 0) -> Status
    {
        Bucket b;
        auto s = tx.create_bucket(options, bname, &b);
        if (s.is_ok()) {
            s = check(tx, b, kv, exists, round);
        }
        return s;
    }

    [[nodiscard]] static auto check_range(const Tx &tx, const Bucket &b, std::size_t kv1, std::size_t kv2, bool exists, std::size_t round = 0) -> Status
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
            for (std::size_t kv = kv1; kv < kv2; ++kv) {
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
                for (std::size_t i = 0; i < kv2 - kv1; ++i) {
                    const auto [k, v] = make_kv(kv2 - i - 1, round);
                    if (i == 0) {
                        c->seek(k);
                    }
                    if (c->is_valid()) {
                        EXPECT_EQ(k, c->key().to_string());
                        EXPECT_EQ(v, c->value().to_string());
                    } else {
                        s = c->status();
                        break;
                    }
                    c->previous();
                }
            }
        } else {
            for (std::size_t kv = kv1; kv < kv2; ++kv) {
                const auto [k, v] = make_kv(kv, round);
                c->seek(k);
                if (c->is_valid()) {
                    EXPECT_NE(k, c->key().to_string());
                } else if (!c->status().is_ok()) {
                    EXPECT_TRUE((c->status().is_io_error()));
                    s = c->status();
                    break;
                }
            }
        }
        delete c;
        return s;
    }
    [[nodiscard]] static auto check_range(const Tx &tx, const std::string &bname, std::size_t kv1, std::size_t kv2, bool exists, std::size_t round = 0) -> Status
    {
        Bucket b;
        auto s = tx.open_bucket(bname, b);
        if (s.is_ok()) {
            s = check_range(tx, b, kv1, kv2, exists, round);
        }
        return s;
    }

    enum Config {
        kDefault = 0,
        kExclusiveLockMode = 1,
        kOffSyncMode = 2,
        kFullSyncMode = 4,
        kUseAltWAL = 8,
        kSmallCache = 16,
        kMaxConfig = 7,
    };
    auto reopen_db(bool clear, Env *env = nullptr) -> Status
    {
        close_db();
        if (clear) {
            (void)DB::destroy(Options(), m_db_name);
        }
        Options options;
        options.busy = &m_busy;
        options.env = env ? env : m_env;
        if (m_config & kExclusiveLockMode) {
            options.lock_mode = Options::kLockExclusive;
        }
        if (m_config & kOffSyncMode) {
            options.sync_mode = Options::kSyncOff;
        } else if (m_config & kFullSyncMode) {
            options.sync_mode = Options::kSyncFull;
        }
        if (m_config & kUseAltWAL) {
            options.wal_filename = m_alt_wal_name;
        }
        if (m_config & kSmallCache) {
            options.cache_size = 0;
        }
        return DB::open(options, m_db_name, m_db);
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
    do {
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
    } while (change_options(true));
}

TEST_F(DBTests, UpdateThenView)
{
    std::size_t round = 0;
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
    std::size_t round = 0;
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
    do {
        ASSERT_OK(m_db->update([](auto &tx) {
            return tx.vacuum();
        }));
    } while (change_options(true));
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
    ASSERT_OK(env->new_file(m_db_name, Env::kReadWrite, file));

    // Corrupt the root ID written to the schema bucket, which has already been
    // written back to the database file. The root ID is a 1 byte varint pointing
    // to page 3. Just increment it, which makes a root that points past the end
    // of the file, which is not allowed.
    char buffer[kPageSize];
    ASSERT_OK(file->read_exact(0, sizeof(buffer), buffer));
    buffer[kPageSize - 1] = 42; // Corrupt the root ID of "BUCKET".
    ASSERT_OK(file->write(0, Slice(buffer, kPageSize)));
    delete file;

    // The pager won't reread pages unless another connection has changed the
    // database. Reopen the database to force it to read the corrupted page.
    ASSERT_OK(reopen_db(false));

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
        return tx.create_bucket(BucketOptions(), "BUCKET", nullptr);
    }));
    ASSERT_EQ(0, file_size(m_db_name));

    ASSERT_OK(m_db->checkpoint(true));
    ASSERT_EQ(kPageSize * 3, file_size(m_db_name));

    ASSERT_OK(m_db->update([](auto &tx) {
        auto s = tx.drop_bucket("BUCKET");
        if (s.is_ok()) {
            s = tx.vacuum();
        }
        return s;
    }));
    ASSERT_EQ(kPageSize * 3, file_size(m_db_name));

    // Tx::vacuum() never gets rid of the root database page, even if the whole database
    // is empty.
    ASSERT_OK(m_db->checkpoint(true));
    ASSERT_EQ(kPageSize, file_size(m_db_name));
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
    FakeEnv env;
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

// TEST(DestructionTests, OnlyDeletesCalicoDatabases)
//{
//     std::filesystem::remove_all("./testdb");
//
//     // "./testdb" does not exist.
//     ASSERT_TRUE(DB::destroy(Options(), "./testdb").is_invalid_argument());
//     ASSERT_FALSE(Env::default_env()->file_exists("./testdb"));
//
//     // File is too small to read the first page.
//     File *file;
//     ASSERT_OK(Env::default_env()->new_file("./testdb", Env::kCreate, file));
//     ASSERT_OK(file->write(0, "CalicoDB format"));
//     ASSERT_TRUE(DB::destroy(Options(), "./testdb").is_invalid_argument());
//     ASSERT_TRUE(Env::default_env()->file_exists("./testdb"));
//
//     // Identifier is incorrect.
//     ASSERT_OK(file->write(0, "CalicoDB format 0"));
//     ASSERT_TRUE(DB::destroy(Options(), "./testdb").is_invalid_argument());
//
//     DB *db;
//     std::filesystem::remove_all("./testdb");
//     ASSERT_OK(DB::open(Options(), "./testdb", db));
//     ASSERT_OK(DB::destroy(Options(), "./testdb"));
//
//     delete db;
//     delete file;
// }

TEST(DestructionTests, OnlyDeletesCalicoWals)
{
    Options options;
    options.env = new FakeEnv;
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

class DBOpenTests : public DBTests
{
protected:
    ~DBOpenTests() override = default;

    auto SetUp() -> void override
    {
        // Don't call DBTests::SetUp(). DB is opened in test body.
    }
};

TEST_F(DBOpenTests, HandlesEmptyFilename)
{
    ASSERT_NOK(DB::open(Options(), "", m_db));
}

TEST_F(DBOpenTests, CreatesMissingDb)
{
    Options options;
    options.error_if_exists = false;
    options.create_if_missing = true;
    ASSERT_OK(DB::open(options, m_db_name, m_db));
    delete m_db;
    m_db = nullptr;

    options.create_if_missing = false;
    ASSERT_OK(DB::open(options, m_db_name, m_db));
}

TEST_F(DBOpenTests, FailsIfMissingDb)
{
    Options options;
    options.create_if_missing = false;
    ASSERT_TRUE(DB::open(options, m_db_name, m_db).is_invalid_argument());
}

TEST_F(DBOpenTests, FailsIfDbExists)
{
    Options options;
    options.create_if_missing = true;
    options.error_if_exists = true;
    ASSERT_OK(DB::open(options, m_db_name, m_db));
    delete m_db;
    m_db = nullptr;

    options.create_if_missing = false;
    ASSERT_TRUE(DB::open(options, m_db_name, m_db).is_invalid_argument());
}
//
// class DBTransactionTests : public DBErrorTests
//{
// protected:
//    explicit DBTransactionTests() = default;
//
//    ~DBTransactionTests() override = default;
//};
//
// TEST_F(DBTransactionTests, ReadMostRecentSnapshot)
//{
//    U64 key_limit = 0;
//    auto should_exist = false;
//    ASSERT_OK(try_reopen(kOpenPrefill));
//    const auto intercept = [this, &key_limit, &should_exist] {
//        DB *db;
//        Options options;
//        options.env = m_test_env;
//        auto s = DB::open(options, m_db_name, db);
//        if (!s.is_ok()) {
//            return s;
//        }
//        s = db->view([key_limit](auto &tx) {
//            return check_range(tx, "BUCKET", 0, key_limit * 10, true);
//        });
//        if (!should_exist && s.is_invalid_argument()) {
//            s = Status::ok();
//        }
//        delete db;
//        return s;
//    };
//    m_test_env->add_interceptor(kWALName, Interceptor(kSyscallWrite, intercept));
//    ASSERT_OK(m_db->update([&should_exist, &key_limit](auto &tx) {
//        for (std::size_t i = 0; i < 50; ++i) {
//            EXPECT_OK(put_range(tx, BucketOptions(), "BUCKET", i * 10, (i + 1) * 10));
//            EXPECT_OK(tx.commit());
//            should_exist = true;
//            key_limit = i + 1;
//        }
//        return Status::ok();
//    }));
//}
//
// TEST_F(DBTransactionTests, ExclusiveLockingMode)
//{
//    for (int i = 0; i < 2; ++i) {
//        m_config = i == 0 ? kExclusiveLockMode : kDefault;
//        ASSERT_OK(try_reopen(kOpenNormal));
//        const auto intercept = [this, &i] {
//            DB *db;
//            Options options;
//            options.lock_mode = i == 0 ? Options::kLockNormal
//                                       : Options::kLockExclusive;
//            options.env = m_test_env;
//
//            Status s;
//            EXPECT_TRUE((s = DB::open(options, m_db_name, db)).is_busy())
//                << s.to_string();
//            return Status::ok();
//        };
//        m_test_env->add_interceptor(kWALName, Interceptor(kSyscallWrite, intercept));
//        ASSERT_OK(m_db->update([](auto &tx) {
//            for (std::size_t i = 0; i < 50; ++i) {
//                EXPECT_OK(put_range(tx, BucketOptions(), "BUCKET", i * 10, (i + 1) * 10));
//                EXPECT_OK(tx.commit());
//            }
//            return Status::ok();
//        }));
//    }
//}
//
// TEST_F(DBTransactionTests, IgnoresFutureVersions)
//{
//    static constexpr U64 kN = 300;
//    auto has_open_db = false;
//    U64 n = 0;
//
//    ASSERT_OK(try_reopen(kOpenPrefill));
//    const auto intercept = [this, &has_open_db, &n] {
//        if (has_open_db || n >= kN) {
//            // Prevent this callback from being called by itself, and prevent the test from
//            // running for too long.
//            return Status::ok();
//        }
//        DB *db;
//        Options options;
//        options.env = m_test_env;
//        has_open_db = true;
//        EXPECT_OK(DB::open(options, m_db_name, db));
//        EXPECT_OK(db->update([n](auto &tx) {
//            return put_range(tx, BucketOptions(), "BUCKET", kN * n, kN * (n + 1));
//        }));
//        delete db;
//        has_open_db = false;
//        ++n;
//        return Status::ok();
//    };
//    ASSERT_OK(m_db->update([](auto &tx) {
//        return put_range(tx, BucketOptions(), "BUCKET", 0, kN);
//    }));
//    m_test_env->add_interceptor(kWALName, Interceptor(kSyscallRead, intercept));
//    (void)m_db->view([&n](auto &tx) {
//        for (std::size_t i = 0; i < kN; ++i) {
//            EXPECT_OK(check_range(tx, "BUCKET", 0, kN, true));
//            EXPECT_OK(check_range(tx, "BUCKET", kN, kN * (n + 1), false));
//        }
//        return Status::ok();
//    });
//}
//
// class DBCheckpointTests : public DBErrorTests
//{
// protected:
//    explicit DBCheckpointTests() = default;
//
//    ~DBCheckpointTests() override = default;
//};
//
// TEST_F(DBCheckpointTests, CheckpointerBlocksOtherCheckpointers)
//{
//    ASSERT_OK(try_reopen(kOpenPrefill));
//    m_test_env->add_interceptor(
//        m_db_name,
//        Interceptor(kSyscallWrite, [this] {
//            // Each time File::write() is called, use a different connection to attempt a
//            // checkpoint. It should get blocked every time, since a checkpoint is already
//            // running.
//            DB *db;
//            Options options;
//            options.env = m_test_env;
//            EXPECT_OK(DB::open(options, m_db_name, db));
//            EXPECT_TRUE(db->checkpoint(false).is_busy());
//            EXPECT_TRUE(db->checkpoint(true).is_busy());
//            delete db;
//            return Status::ok();
//        }));
//    ASSERT_OK(m_db->checkpoint(true));
//}
//
// TEST_F(DBCheckpointTests, CheckpointerAllowsTransactions)
//{
//    static constexpr std::size_t kCkptCount = 1'000;
//
//    // Set up a DB with some records in both the database file and the WAL.
//    ASSERT_OK(try_reopen(kOpenPrefill));
//    ASSERT_OK(m_db->checkpoint(true));
//    ASSERT_OK(m_db->update([](auto &tx) {
//        // These records will be checkpointed below. round is 1 to cause a new version of the first half of
//        // the records to be written.
//        return put_range(tx, BucketOptions(), "saved", 0, kSavedCount / 2, 1);
//    }));
//
//    U64 n = 0;
//    m_test_env->add_interceptor(
//        m_db_name,
//        Interceptor(kSyscallWrite, [this, &n] {
//            DB *db;
//            Options options;
//            options.env = m_test_env;
//            EXPECT_OK(DB::open(options, m_db_name, db));
//            EXPECT_OK(db->update([n](auto &tx) {
//                return put_range(tx, BucketOptions(), "SELF", n * 2, (n + 1) * 2);
//            }));
//            (void)db->view([n](auto &tx) {
//                EXPECT_OK(check_range(tx, "saved", 0, kSavedCount / 2, true, 1));
//                EXPECT_OK(check_range(tx, "saved", kSavedCount / 2, kSavedCount, true, 0));
//                EXPECT_OK(check_range(tx, "SELF", 0, (n + 1) * 2, true));
//                return Status::ok();
//            });
//            ++n;
//            delete db;
//            return Status::ok();
//        }));
//    ASSERT_OK(m_db->checkpoint(false));
//}

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
                b = static_cast<U8>((b << 1) | (b >> 7));
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

} // namespace calicodb::test
