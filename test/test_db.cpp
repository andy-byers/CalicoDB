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

class CallbackEnv : public EnvWrapper
{
public:
    std::function<void()> m_read_callback;
    std::function<void()> m_write_callback;
    bool m_in_callback = false;

    auto call_read_callback() -> void
    {
        if (m_read_callback && !m_in_callback) {
            m_in_callback = true;
            m_read_callback();
            m_in_callback = false;
        }
    }
    auto call_write_callback() -> void
    {
        if (m_write_callback && !m_in_callback) {
            m_in_callback = true;
            m_write_callback();
            m_in_callback = false;
        }
    }

    explicit CallbackEnv(Env &env)
        : EnvWrapper(env)
    {
    }

    ~CallbackEnv() override = default;

    auto new_file(const std::string &filename, OpenMode mode, File *&file_out) -> Status override
    {
        class CallbackFile : public FileWrapper
        {
            CallbackEnv *m_env;

        public:
            explicit CallbackFile(CallbackEnv &env, File &base)
                : FileWrapper(base),
                  m_env(&env)
            {
            }

            ~CallbackFile() override
            {
                delete m_target;
            }

            auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status override
            {
                m_env->call_read_callback();
                return FileWrapper::read(offset, size, scratch, out);
            }

            auto write(std::size_t offset, const Slice &in) -> Status override
            {
                m_env->call_write_callback();
                return FileWrapper::write(offset, in);
            }
        };

        auto s = target()->new_file(filename, mode, file_out);
        if (s.is_ok()) {
            file_out = new CallbackFile(*this, *file_out);
        }
        return s;
    }
};

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
          m_env(new CallbackEnv(Env::default_env()))
    {
        (void)DB::destroy(Options(), m_db_name);
    }

    ~DBTests() override
    {
        delete m_db;
        delete m_env;
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
        kInMemory = 32,
        kMaxConfig,
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
        if (m_config & kInMemory) {
            options.temp_database = true;
            options.env = nullptr;
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
    CallbackEnv *m_env = nullptr;
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
        EXPECT_OK(tx.put(b, "key", "value"));
        std::string value;
        EXPECT_OK(tx.get(b, "key", &value));
        EXPECT_EQ("value", value);
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

TEST_F(DBTests, RollbackRootUpdate)
{
    do {
        ASSERT_TRUE(m_db->update([](auto &tx) {
                            Bucket b;
                            for (std::size_t i = 0; i < 10; ++i) {
                                auto s = tx.create_bucket(BucketOptions(), numeric_key(i), &b);
                                if (!s.is_ok()) {
                                    return s;
                                }
                                if (i == 5) {
                                    s = tx.commit();
                                    if (!s.is_ok()) {
                                        return s;
                                    }
                                }
                            }
                            return Status::not_found("42");
                        })
                        .to_string() == "not found: 42");
        ASSERT_OK(m_db->view([](const auto &tx) {
            Bucket b;
            Status s;
            for (std::size_t i = 0; i < 10 && s.is_ok(); ++i) {
                s = tx.open_bucket(numeric_key(i), b);
                if (i <= 5) {
                    EXPECT_OK(s);
                } else {
                    EXPECT_NOK(s);
                    s = Status::ok();
                }
            }
            return s;
        }));
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
    auto &env = Env::default_env();
    ASSERT_OK(env.new_file(m_db_name, Env::kReadWrite, file));

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

TEST_F(DBTests, AutoCheckpoint)
{
    Options options;
    for (std::size_t i = 1; i < 100; i += i) {
        delete m_db;
        m_db = nullptr;

        options.auto_checkpoint = i;
        ASSERT_OK(DB::open(options, m_db_name, m_db));
        for (std::size_t j = 0; j < 10; ++j) {
            ASSERT_OK(m_db->update([j](auto &tx) {
                return put_range(tx, BucketOptions(), "b", j * 1'000, (j + 1) * 1'000);
            }));
        }
    }
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

TEST_F(DBTests, BucketExistence)
{
    // Cannot Tx::open_bucket() a bucket that doesn't exist, even in a read-write transaction.
    const auto try_open_bucket = [](const auto &tx) {
        Bucket b;
        return tx.open_bucket("bucket", b);
    };
    ASSERT_NOK(m_db->view(try_open_bucket));
    ASSERT_NOK(m_db->update(try_open_bucket));

    // Tx::create_bucket() must be used for bucket creation.
    ASSERT_OK(m_db->update([](auto &tx) {
        return tx.create_bucket(BucketOptions(), "bucket", nullptr);
    }));

    ASSERT_NOK(m_db->update([](auto &tx) {
        BucketOptions b_opt;
        b_opt.error_if_exists = true;
        return tx.create_bucket(b_opt, "bucket", nullptr);
    }));

    // Now the bucket can be opened.
    ASSERT_OK(m_db->view([](auto &tx) {
        Bucket b;
        return tx.open_bucket("bucket", b);
    }));
}

TEST_F(DBTests, SpaceAmplification)
{
    static constexpr std::size_t kInputSize = 1'024 * 1'024;
    static constexpr std::size_t kNumRecords = kInputSize / 256;

    RandomGenerator random;
    ASSERT_OK(m_db->update([&random](auto &tx) {
        Bucket b;
        auto s = tx.create_bucket(BucketOptions(), "b", &b);
        for (std::size_t i = 0; s.is_ok() && i < kNumRecords; ++i) {
            const auto key = random.Generate(kInputSize / kNumRecords / 2);
            const auto val = random.Generate(key.size());
            s = tx.put(b, key, val);
        }
        return s;
    }));

    close_db();
    std::size_t file_size;
    ASSERT_OK(m_env->file_size(m_db_name, file_size));
    const auto space_amp = static_cast<double>(file_size) / static_cast<double>(kInputSize);
    std::cout << "SpaceAmplification: " << space_amp << '\n';
}

TEST_F(DBTests, VacuumDroppedBuckets)
{
    ASSERT_OK(m_db->update([](auto &tx) {
        EXPECT_OK(put_range(tx, BucketOptions(), "a", 0, 1'000));
        EXPECT_OK(put_range(tx, BucketOptions(), "b", 0, 1'000));
        EXPECT_OK(put_range(tx, BucketOptions(), "c", 0, 1'000));
        return Status::ok();
    }));
    ASSERT_OK(m_db->update([](auto &tx) {
        Bucket a, b, c;
        EXPECT_OK(tx.open_bucket("a", a));
        EXPECT_OK(tx.open_bucket("b", b));
        EXPECT_OK(tx.open_bucket("c", c));
        EXPECT_OK(tx.drop_bucket("a"));
        EXPECT_OK(tx.drop_bucket("c"));
        return tx.vacuum();
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

TEST(DestructionTests, OnlyDeletesCalicoDatabases)
{
    (void)Env::default_env().remove_file("./testdb");

    // "./testdb" does not exist.
    ASSERT_NOK(DB::destroy(Options(), "./testdb"));
    ASSERT_FALSE(Env::default_env().file_exists("./testdb"));

    // File is too small to read the first page.
    File *file;
    ASSERT_OK(Env::default_env().new_file("./testdb", Env::kCreate, file));
    ASSERT_OK(file->write(0, "CalicoDB format"));
    ASSERT_NOK(DB::destroy(Options(), "./testdb"));
    ASSERT_TRUE(Env::default_env().file_exists("./testdb"));

    // Identifier is incorrect.
    ASSERT_OK(file->write(0, "CalicoDB format 0"));
    ASSERT_NOK(DB::destroy(Options(), "./testdb"));

    ASSERT_OK(Env::default_env().remove_file("./testdb"));

    DB *db;
    ASSERT_OK(DB::open(Options(), "./testdb", db));
    ASSERT_OK(DB::destroy(Options(), "./testdb"));

    delete db;
    delete file;
}

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

TEST(DestructionTests, DeletesWalAndShm)
{
    Options options;
    options.env = new FakeEnv;

    DB *db;
    // Create the DB file.
    ASSERT_OK(DB::open(options, "./test", db));
    delete db;

    File *file;
    // Create fake WAL and shm files.
    ASSERT_OK(options.env->new_file("./test-wal", Env::kCreate, file));
    delete file;
    ASSERT_OK(options.env->new_file("./test-shm", Env::kCreate, file));
    delete file;

    ASSERT_TRUE(options.env->file_exists("./test"));
    ASSERT_TRUE(options.env->file_exists("./test-wal"));
    ASSERT_TRUE(options.env->file_exists("./test-shm"));

    ASSERT_OK(DB::destroy(options, "./test"));

    ASSERT_FALSE(options.env->file_exists("./test"));
    ASSERT_FALSE(options.env->file_exists("./test-wal"));
    ASSERT_FALSE(options.env->file_exists("./test-shm"));

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

TEST_F(DBOpenTests, CustomLogger)
{
    class : public Logger
    {
    public:
        std::string m_str;

        auto logv(const char *fmt, std::va_list args) -> void override
        {
            char fixed[1'024];
            std::va_list args_copy;
            va_copy(args_copy, args);

            const auto offset = m_str.size();
            const auto len = std::vsnprintf(
                fixed, sizeof(fixed), fmt, args);
            ASSERT_TRUE(0 <= len && len < 1'024);
            m_str.append(fixed);
            va_end(args_copy);
        }
    } logger;

    Options options;
    options.info_log = &logger;
    // DB will warn (through the log) about the fact that options.env is not nullptr.
    // It will clear that field and use it to hold a custom Env that helps implement
    // in-memory databases.
    options.env = &Env::default_env();
    options.temp_database = true;
    // In-memory databases can have an empty filename.
    ASSERT_OK(DB::open(options, "", m_db));
    delete m_db;
    m_db = nullptr;

    ASSERT_FALSE(logger.m_str.empty());
}

class TransactionTests : public DBTests
{
protected:
    explicit TransactionTests() = default;

    ~TransactionTests() override = default;
};

TEST_F(TransactionTests, ReadsMostRecentSnapshot)
{
    U64 key_limit = 0;
    auto should_records_exist = false;
    auto should_database_exist = true;
    m_env->m_write_callback = [&] {
        DB *db;
        Options options;
        options.env = m_env;
        auto s = DB::open(options, m_db_name, db);
        if (!should_database_exist && s.is_busy()) {
            // This happens when this callback is called by a write during the final checkpoint, after an exclusive
            // lock has been taken on the database file to make sure we are the last connection. It is too late at
            // this point, the checkpoint will run and the WAL will be unlinked.
            return;
        }
        s = db->view([key_limit](auto &tx) {
            return check_range(tx, "BUCKET", 0, key_limit, true);
        });
        delete db;
        if (!should_records_exist && s.is_invalid_argument()) {
            s = Status::ok();
        }
        ASSERT_OK(s);
    };
    ASSERT_OK(m_db->update([&](auto &tx) {
        for (std::size_t i = 0; i < 50; ++i) {
            static constexpr U64 kScale = 10;
            EXPECT_OK(put_range(tx, BucketOptions(), "BUCKET", i * kScale, (i + 1) * kScale));
            EXPECT_OK(tx.commit());
            should_records_exist = true;
            key_limit = (i + 1) * kScale;
        }
        should_database_exist = false;
        return Status::ok();
    }));

    should_database_exist = true;
    m_env->m_write_callback();
}

TEST_F(TransactionTests, ExclusiveLockingMode)
{
    for (int i = 0; i < 2; ++i) {
        m_config = i == 0 ? kExclusiveLockMode : kDefault;
        ASSERT_OK(reopen_db(false));
        m_env->m_write_callback = [this, &i] {
            DB *db;
            Options options;
            options.lock_mode = i == 0 ? Options::kLockNormal
                                       : Options::kLockExclusive;
            options.env = m_env;

            Status s;
            ASSERT_TRUE((s = DB::open(options, m_db_name, db)).is_busy())
                << s.to_string();
        };
        ASSERT_OK(m_db->update([](auto &tx) {
            for (std::size_t i = 0; i < 50; ++i) {
                EXPECT_OK(put_range(tx, BucketOptions(), "BUCKET", i * 10, (i + 1) * 10));
                EXPECT_OK(tx.commit());
            }
            return Status::ok();
        }));
        m_env->m_write_callback = {};
    }
}

TEST_F(TransactionTests, IgnoresFutureVersions)
{
    static constexpr U64 kN = 5;
    auto has_open_db = false;
    U64 n = 0;

    ASSERT_OK(m_db->update([](auto &tx) {
        return put_range(tx, BucketOptions(), "BUCKET", 0, kN);
    }));

    m_env->m_read_callback = [this, &has_open_db, &n] {
        if (has_open_db || n >= kN) {
            // Prevent this callback from being called by itself, and prevent the test from
            // running for too long.
            return;
        }
        DB *db;
        Options options;
        options.env = m_env;
        has_open_db = true;
        ASSERT_OK(DB::open(options, m_db_name, db));
        ASSERT_OK(db->update([n](auto &tx) {
            return put_range(tx, BucketOptions(), "BUCKET", kN * n, kN * (n + 1));
        }));
        delete db;
        has_open_db = false;
        ++n;
    };

    (void)m_db->view([&n](auto &tx) {
        for (std::size_t i = 0; i < kN; ++i) {
            EXPECT_OK(check_range(tx, "BUCKET", 0, kN, true));
            EXPECT_OK(check_range(tx, "BUCKET", kN, kN * (n + 1), false));
        }
        return Status::ok();
    });

    m_env->m_read_callback = {};
}

class CheckpointTests : public DBTests
{
protected:
    explicit CheckpointTests() = default;

    ~CheckpointTests() override = default;
};

TEST_F(CheckpointTests, CheckpointerBlocksOtherCheckpointers)
{
    ASSERT_OK(m_db->update([](auto &tx) {
        return put_range(tx, BucketOptions(), "BUCKET", 0, 1'000);
    }));
    m_env->m_write_callback = [this] {
        // Each time File::write() is called, use a different connection to attempt a
        // checkpoint. It should get blocked every time, since a checkpoint is already
        // running.
        DB *db;
        Options options;
        options.env = m_env;
        ASSERT_OK(DB::open(options, m_db_name, db));
        ASSERT_TRUE(db->checkpoint(false).is_busy());
        ASSERT_TRUE(db->checkpoint(true).is_busy());
        delete db;
    };
    ASSERT_OK(m_db->checkpoint(true));
}

TEST_F(CheckpointTests, CheckpointerAllowsTransactions)
{
    static constexpr std::size_t kSavedCount = 100;

    // Set up a DB with some records in both the database file and the WAL.
    ASSERT_OK(m_db->update([](auto &tx) {
        return put_range(tx, BucketOptions(), "before", 0, kSavedCount);
    }));
    ASSERT_OK(m_db->checkpoint(true));
    ASSERT_OK(m_db->update([](auto &tx) {
        // These records will be checkpointed below. round is 1 to cause a new version of the first half of
        // the records to be written.
        return put_range(tx, BucketOptions(), "before", 0, kSavedCount / 2, 1);
    }));

    U64 n = 0;
    m_env->m_write_callback = [this, &n] {
        DB *db;
        Options options;
        options.env = m_env;
        ASSERT_OK(DB::open(options, m_db_name, db));
        ASSERT_OK(db->update([n](auto &tx) {
            return put_range(tx, BucketOptions(), "after", n * 2, (n + 1) * 2);
        }));
        (void)db->view([n](auto &tx) {
            EXPECT_OK(check_range(tx, "before", 0, kSavedCount / 2, true, 1));
            EXPECT_OK(check_range(tx, "before", kSavedCount / 2, kSavedCount, true, 0));
            EXPECT_OK(check_range(tx, "after", 0, (n + 1) * 2, true));
            return Status::ok();
        });
        ++n;
        delete db;
    };

    ASSERT_OK(m_db->checkpoint(false));
    // Don't call the callback during close. DB::open() will return a Status::busy() due to the
    // exclusive lock held.
    m_env->m_write_callback = {};
}

class VacuumTests : public DBTests
{
protected:
    explicit VacuumTests() = default;

    ~VacuumTests() override = default;

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

TEST_F(VacuumTests, SingleBucket)
{
    test_configurations({
        0b10000000,
        0b10000000,
        0b10000000,
        0b10000000,
    });
}

TEST_F(VacuumTests, MultipleBuckets)
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

TEST_F(VacuumTests, SanityCheck)
{
    test_configurations({
        0b11111111,
        0b11111111,
        0b11111111,
        0b11111111,
    });
}

} // namespace calicodb::test
