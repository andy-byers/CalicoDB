// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "common.h"
#include "db_impl.h"
#include "fake_env.h"
#include "header.h"
#include "logging.h"
#include "model.h"
#include "test.h"
#include "tx_impl.h"
#include <filesystem>
#include <gtest/gtest.h>

namespace calicodb::test
{

TEST(FileFormatTests, ReportsUnrecognizedFormatString)
{
    char page[TEST_PAGE_SIZE];
    FileHdr::make_supported_db(page, TEST_PAGE_SIZE);

    ++page[0];
    ASSERT_NOK(FileHdr::check_db_support(page));
}

TEST(FileFormatTests, ReportsUnrecognizedFormatVersion)
{
    char page[TEST_PAGE_SIZE];
    FileHdr::make_supported_db(page, TEST_PAGE_SIZE);

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

    auto new_file(const char *filename, OpenMode mode, File *&file_out) -> Status override
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

            auto read(uint64_t offset, size_t size, char *scratch, Slice *out) -> Status override
            {
                m_env->call_read_callback();
                return FileWrapper::read(offset, size, scratch, out);
            }

            auto write(uint64_t offset, const Slice &in) -> Status override
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
public:
    static constexpr size_t kMaxRounds = 1'000;
    static constexpr size_t kPageSize = TEST_PAGE_SIZE;
    const std::string m_test_dir;
    std::string m_db_name;
    const std::string m_alt_wal_name;

    static constexpr size_t kMaxBuckets = 13;
    static constexpr const char kBucketStr[kMaxBuckets + 2] = "BUCKET_NAMING_";
    enum Config {
        kDefault = 0,
        kExclusiveLockMode = 1,
        kOffSyncMode = 2,
        kFullSyncMode = 4,
        kUseAltWAL = 8,
        kSmallCache = 16,
        kInMemory = 32,
        kMaxConfig,
    } m_config = kDefault;
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

    explicit DBTests()
        : m_test_dir(testing::TempDir()),
          m_db_name(m_test_dir + "calicodb_test_db"),
          m_alt_wal_name(m_db_name + "_alternate_wal"),
          m_env(new CallbackEnv(default_env()))
    {
        remove_calicodb_files(m_db_name);
    }

    ~DBTests() override
    {
        delete m_db;
        delete m_env;
        EXPECT_EQ(DebugAllocator::bytes_used(), 0);
    }

    auto SetUp() -> void override
    {
        ASSERT_OK(reopen_db(false));
    }

    [[nodiscard]] static auto make_kv(size_t kv, size_t round = 0) -> std::pair<std::string, std::string>
    {
        EXPECT_LE(0, kv);
        EXPECT_LE(0, round);
        // 3 pages is long enough to generate both types of overflow pages (kOverflowHead
        // and kOverflowLink).
        static constexpr size_t kMaxKV = TEST_PAGE_SIZE * 3;
        const auto key_length = (round + 1) * kMaxKV / kMaxRounds;
        auto key_str = numeric_key<kMaxKV>(kv);
        key_str = key_str.substr(kMaxKV - key_length);
        const auto val_length = kMaxKV - key_length;
        auto val_str = std::to_string(kv);
        if (val_str.size() < val_length) {
            val_str.resize(TEST_PAGE_SIZE / 4 - val_str.size(), '0');
        }
        return {key_str, val_str};
    }

    [[nodiscard]] static auto put(Bucket &b, size_t kv, size_t round = 0) -> Status
    {
        const auto [k, v] = make_kv(kv, round);
        return b.put(k, v);
    }

    [[nodiscard]] static auto put(Tx &tx, const std::string &bname, size_t kv, size_t round = 0) -> Status
    {
        TestBucket b;
        auto s = test_create_and_open_bucket(tx, bname, b);
        if (s.is_ok()) {
            s = put(*b, kv, round);
        }
        return s;
    }

    [[nodiscard]] static auto put_range(Bucket &b, size_t kv1, size_t kv2, size_t round = 0) -> Status
    {
        Status s;
        for (size_t kv = kv1; s.is_ok() && kv < kv2; ++kv) {
            s = put(b, kv, round);
        }
        return s;
    }
    [[nodiscard]] static auto put_range(Tx &tx, const std::string &bname, size_t kv1, size_t kv2, size_t round = 0) -> Status
    {
        TestBucket b;
        auto s = test_create_and_open_bucket(tx, bname, b);
        if (s.is_ok()) {
            s = put_range(*b, kv1, kv2, round);
        }
        return s;
    }

    [[nodiscard]] static auto erase(Bucket &b, size_t kv, size_t round = 0) -> Status
    {
        const auto [k, _] = make_kv(kv, round);
        return b.erase(k);
    }
    [[nodiscard]] static auto erase(Tx &tx, const std::string &bname, size_t kv, size_t round = 0) -> Status
    {
        TestBucket b;
        auto s = test_create_and_open_bucket(tx, bname, b);
        if (s.is_ok()) {
            s = erase(*b, kv, round);
        }
        return s;
    }

    [[nodiscard]] static auto erase_range(Bucket &b, size_t kv1, size_t kv2, size_t round = 0) -> Status
    {
        Status s;
        for (size_t kv = kv1; s.is_ok() && kv < kv2; ++kv) {
            s = erase(b, kv, round);
        }
        return s;
    }
    [[nodiscard]] static auto erase_range(Tx &tx, const std::string &bname, size_t kv1, size_t kv2, size_t round = 0) -> Status
    {
        TestBucket b;
        auto s = test_create_and_open_bucket(tx, bname, b);
        if (s.is_ok()) {
            s = erase_range(*b, kv1, kv2, round);
        }
        return s;
    }

    [[nodiscard]] static auto check(Cursor &c, size_t kv, bool exists, size_t round = 0) -> Status
    {
        std::string result;
        const auto [k, v] = make_kv(kv, round);
        c.find(k);
        if (c.is_valid()) {
            EXPECT_TRUE(exists);
            uint64_t n;
            Slice slice(result);
            EXPECT_TRUE(consume_decimal_number(slice, &n));
            EXPECT_EQ(kv, n);
        } else if (c.status().is_not_found()) {
            EXPECT_FALSE(exists);
        } else {
            return c.status();
        }
        return Status::ok();
    }
    [[nodiscard]] static auto check(Tx &tx, const std::string &bname, size_t kv, bool exists, size_t round = 0) -> Status
    {
        TestBucket b;
        auto s = test_create_and_open_bucket(tx, bname, b);
        if (s.is_ok()) {
            auto c = test_new_cursor(*b);
            s = check(*c, kv, exists, round);
        }
        return s;
    }

    [[nodiscard]] static auto check_range(Cursor &c, size_t kv1, size_t kv2, bool exists, size_t round = 0) -> Status
    {
        // Run some extra seek*() calls.
        if (kv1 & 1) {
            c.seek_first();
        } else {
            c.seek_last();
        }
        Status s;
        if (c.status().is_io_error()) {
            s = c.status();
        }
        if (s.is_ok() && exists) {
            for (size_t kv = kv1; kv < kv2; ++kv) {
                const auto [k, v] = make_kv(kv, round);
                if (kv == kv1) {
                    c.seek(k);
                }
                if (c.is_valid()) {
                    EXPECT_EQ(k, to_string(c.key()));
                    EXPECT_EQ(v, to_string(c.value()));
                } else {
                    EXPECT_TRUE(c.status().is_io_error());
                    s = c.status();
                    break;
                }
                c.next();
            }
            if (s.is_ok()) {
                for (size_t i = 0; i < kv2 - kv1; ++i) {
                    const auto [k, v] = make_kv(kv2 - i - 1, round);
                    if (i == 0) {
                        c.seek(k);
                    }
                    if (c.is_valid()) {
                        EXPECT_EQ(k, to_string(c.key()));
                        EXPECT_EQ(v, to_string(c.value()));
                    } else {
                        s = c.status();
                        break;
                    }
                    c.previous();
                }
            }
        } else {
            for (size_t kv = kv1; kv < kv2; ++kv) {
                const auto [k, v] = make_kv(kv, round);
                c.seek(k);
                if (c.is_valid()) {
                    EXPECT_NE(k, to_string(c.key()));
                } else if (!c.status().is_ok()) {
                    EXPECT_TRUE((c.status().is_io_error()));
                    s = c.status();
                    break;
                }
            }
        }
        return s;
    }
    [[nodiscard]] static auto check_range(const Tx &tx, const std::string &bname, size_t kv1, size_t kv2, bool exists, size_t round = 0) -> Status
    {
        TestBucket b;
        auto s = test_open_bucket(tx, bname, b);
        if (s.is_ok()) {
            auto c = test_new_cursor(*b);
            s = check_range(*c, kv1, kv2, exists, round);
        }
        return s;
    }

    auto reopen_db(bool clear, Env *env = nullptr) -> Status
    {
        close_db();
        Options options;
        options.busy = &m_busy;
        options.env = env ? env : m_env;
        options.page_size = TEST_PAGE_SIZE;
        if (clear) {
            remove_calicodb_files(m_db_name);
            std::filesystem::remove_all(m_alt_wal_name);
        }
        if (m_config & kExclusiveLockMode) {
            options.lock_mode = Options::kLockExclusive;
        }
        if (m_config & kOffSyncMode) {
            options.sync_mode = Options::kSyncOff;
        } else if (m_config & kFullSyncMode) {
            options.sync_mode = Options::kSyncFull;
        }
        if (m_config & kUseAltWAL) {
            options.wal_filename = m_alt_wal_name.c_str();
        }
        if (m_config & kSmallCache) {
            options.cache_size = 0;
        }
        if (m_config & kInMemory) {
            options.temp_database = true;
            options.env = nullptr;
        }
        return DB::open(options, m_db_name.c_str(), m_db);
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
        return m_config < kMaxConfig;
    }

    [[nodiscard]] auto file_size(const char *filename) const -> size_t
    {
        File *file;
        uint64_t file_size = 0;
        auto s = m_env->new_file(filename, Env::kReadOnly, file);
        if (s.is_ok()) {
            EXPECT_OK(file->get_size(file_size));
            delete file;
        }
        return file_size;
    }
};

TEST_F(DBTests, GetProperty)
{
    std::string value;
    ASSERT_NOK(m_db->get_property("nonexistent", nullptr));
    ASSERT_NOK(m_db->get_property("nonexistent", &value));
    ASSERT_TRUE(value.empty());
}

TEST_F(DBTests, ConvenienceFunctions)
{
    (void)reinterpret_cast<DBImpl *>(m_db)->TEST_pager();
    ASSERT_OK(m_db->update([](auto &tx) {
        // Let the database root page get initialized.
        EXPECT_OK(tx.create_bucket("bucket", nullptr));
        reinterpret_cast<TxImpl *>(&tx)->TEST_validate();
        return Status::ok();
    }));
}

TEST_F(DBTests, NewTx)
{
    // Junk addresses used to make sure new_tx() clears its out pointer parameter
    // on failure.
    Tx *reader1, *reader2 = reinterpret_cast<Tx *>(42);
    Tx *writer1, *writer2 = reinterpret_cast<Tx *>(42);

    ASSERT_OK(m_db->new_writer(writer1));
    ASSERT_NE(nullptr, writer2);
    ASSERT_NOK(m_db->new_writer(writer2));
    ASSERT_EQ(nullptr, writer2);
    delete writer1;

    ASSERT_OK(m_db->new_writer(writer2));
    ASSERT_NE(nullptr, reader2);
    ASSERT_NOK(m_db->new_reader(reader2));
    ASSERT_EQ(nullptr, reader2);
    delete writer2;

    ASSERT_OK(m_db->new_reader(reader2));
    ASSERT_NE(nullptr, writer2);
    ASSERT_NOK(m_db->new_writer(writer2));
    ASSERT_EQ(nullptr, writer2);
    delete reader2;

    ASSERT_OK(m_db->new_reader(reader1));
    ASSERT_NE(nullptr, reader2);
    ASSERT_NOK(m_db->new_reader(reader2));
    ASSERT_EQ(nullptr, reader2);
    delete reader1;

    ASSERT_OK(m_db->update([](auto &tx) {
        return put_range(tx, "bucket", 0, 100);
    }));
    ASSERT_OK(m_db->view([](const auto &tx) {
        return check_range(tx, "bucket", 0, 100, true);
    }));
}

TEST_F(DBTests, NewBucket)
{
    ASSERT_OK(m_db->update([](auto &tx) {
        TestBucket b;
        EXPECT_NOK(test_open_bucket(tx, "b", b));
        EXPECT_OK(test_create_and_open_bucket(tx, "b", b));
        return Status::ok();
    }));
}

TEST_F(DBTests, BucketBehavior)
{
    ASSERT_OK(m_db->update([](auto &tx) {
        TestBucket b;
        EXPECT_OK(test_create_and_open_bucket(tx, "b", b));
        EXPECT_OK(b->put("key", "value"));
        auto c = test_new_cursor(*b);
        c->find("key");
        EXPECT_OK(c->status());
        EXPECT_EQ("value", c->value());
        return Status::ok();
    }));
}

TEST_F(DBTests, ReadonlyTxDisallowsWrites)
{
    ASSERT_OK(m_db->update([](auto &tx) {
        return tx.create_bucket("b", nullptr);
    }));

    Tx *tx;
    ASSERT_OK(m_db->new_reader(tx));

    TestBucket b;
    ASSERT_NOK(test_create_and_open_bucket(*tx, "b", b));
    ASSERT_OK(test_open_bucket(*tx, "b", b));

    ASSERT_EQ(b->put("key", "value").code(), Status::kNotSupported);
    ASSERT_EQ(b->erase("key").code(), Status::kNotSupported);
    ASSERT_EQ(b->drop_bucket("b").code(), Status::kNotSupported);
    ASSERT_EQ(tx->vacuum().code(), Status::kNotSupported);

    b.reset();
    delete tx;
}

TEST_F(DBTests, ReadonlyTx)
{
    do {
        ASSERT_OK(m_db->update([](auto &tx) {
            return tx.create_bucket("b", nullptr);
        }));
        ASSERT_OK(m_db->view([](const auto &tx) {
            TestBucket b;
            return test_open_bucket(tx, "b", b);
        }));
    } while (change_options(true));
}

TEST_F(DBTests, UpdateThenView)
{
    size_t round = 0;
    do {
        if (!(m_config & kInMemory)) {
            continue;
        }
        for (int i = 0; i < 3; ++i) {
            ASSERT_OK(m_db->update([i, round](auto &tx) {
                TestBucket b;
                auto s = test_create_and_open_bucket(tx, kBucketStr + i, b);
                if (s.is_ok()) {
                    s = put_range(*b, 0, 1'000, round);
                    if (s.is_ok()) {
                        s = erase_range(*b, 250, 750, round);
                    }
                }
                return s;
            }));
        }
        for (int i = 0; i < 3; ++i) {
            ASSERT_OK(m_db->view([round, i](const auto &tx) {
                TestBucket b;
                auto s = test_open_bucket(tx, kBucketStr + i, b);
                if (s.is_ok()) {
                    auto c = test_new_cursor(*b);
                    EXPECT_OK(check_range(*c, 0, 250, true, round));
                    EXPECT_OK(check_range(*c, 250, 750, false, round));
                    EXPECT_OK(check_range(*c, 750, 1'000, true, round));
                }
                return s;
            }));
        }
        ASSERT_OK(m_db->update([](auto &tx) {
            return tx.vacuum();
        }));
        ASSERT_OK(m_db->checkpoint(kCheckpointPassive, nullptr));
        ++round;
    } while (change_options(true));
}

TEST_F(DBTests, RollbackRootUpdate)
{
    do {
        ASSERT_EQ(0, std::strcmp(m_db->update([](auto &tx) {
                                         TestBucket b;
                                         for (size_t i = 0; i < 10; ++i) {
                                             auto s = test_create_and_open_bucket(tx, numeric_key(i).c_str(), b);
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
                                     .message(),
                                 "42"));
        ASSERT_OK(m_db->view([](const auto &tx) {
            Status s;
            TestBucket b;
            for (size_t i = 0; i < 10 && s.is_ok(); ++i) {
                s = test_open_bucket(tx, numeric_key(i).c_str(), b);
                if (i <= 5) {
                    EXPECT_OK(s);
                } else {
                    EXPECT_NOK(s);
                    EXPECT_FALSE(b);
                    s = Status::ok();
                }
            }
            return s;
        }));
    } while (change_options(true));
}

TEST_F(DBTests, RollbackUpdate)
{
    size_t round = 0;
    do {
        for (int i = 0; i < 3; ++i) {
            const auto s = m_db->update([i, round](auto &tx) {
                TestBucket b;
                auto s = test_create_and_open_bucket(tx, kBucketStr + i, b);
                if (s.is_ok()) {
                    s = put_range(*b, 0, 500, round);
                    if (s.is_ok()) {
                        // We have access to the Tx here, so we can actually call
                        // Tx::commit() as many times as we want before we return.
                        // The returned status determines whether to perform a final
                        // commit before calling delete on the Tx.
                        s = tx.commit();
                        if (s.is_ok()) {
                            s = put_range(*b, 500, 1'000, round);
                            if (s.is_ok()) {
                                // Cause the rest of the changes to be rolled back.
                                return Status::not_found("42");
                            }
                        }
                    }
                }
                return s;
            });
            ASSERT_EQ(0, std::strcmp(s.message(), "42"));
        }
        for (int i = 0; i < 3; ++i) {
            ASSERT_OK(m_db->view([round, i](const auto &tx) {
                TestBucket b;
                auto s = test_open_bucket(tx, kBucketStr + i, b);
                if (s.is_ok()) {
                    auto c = test_new_cursor(*b);
                    EXPECT_OK(check_range(*c, 0, 500, true, round));
                    EXPECT_OK(check_range(*c, 500, 1'000, false, round));
                }
                return s;
            }));
        }
        ASSERT_OK(m_db->checkpoint(kCheckpointPassive, nullptr));
        ++round;
    } while (change_options(true));
}

TEST_F(DBTests, ModifyRecordSpecialCase)
{
    const std::string long_key(kPageSize - NodeHdr::kSize - kMaxCellHeaderSize - 2, '*');
    ASSERT_OK(m_db->update([&long_key](auto &tx) {
        TestBucket b;
        EXPECT_OK(test_create_and_open_bucket(tx, "b", b));
        EXPECT_OK(b->put(long_key, "old_value"));
        EXPECT_OK(tx.commit());
        EXPECT_OK(b->put(long_key, "new_value"));
        return Status::ok();
    }));
    ASSERT_OK(m_db->view([&long_key](auto &tx) {
        TestBucket b;
        EXPECT_OK(test_open_bucket(tx, "b", b));

        auto c = test_new_cursor(*b);
        c->find(long_key);
        EXPECT_TRUE(c->is_valid());
        EXPECT_EQ(c->value(), "new_value");
        return Status::ok();
    }));
}

TEST_F(DBTests, VacuumEmptyDB)
{
    do {
        ASSERT_OK(m_db->update([](auto &tx) {
            return tx.vacuum();
        }));
    } while (change_options(true));
}

TEST_F(DBTests, AutoCheckpoint)
{
    static constexpr size_t kN = 1'000;
    Options options;
    for (size_t i = 1; i < 100; i += i) {
        delete exchange(m_db, nullptr);

        options.auto_checkpoint = i;
        ASSERT_OK(DB::open(options, m_db_name.c_str(), m_db));
        for (size_t j = 0; j < 100; ++j) {
            ASSERT_OK(m_db->update([i, j](auto &tx) {
                return put_range(tx, "b", j * kN, (j + 1) * kN, i);
            }));
        }
    }
}

TEST_F(DBTests, CheckpointResize)
{
    ASSERT_OK(m_db->update([](auto &tx) {
        return tx.create_bucket("b", nullptr);
    }));
    ASSERT_EQ(0, file_size(m_db_name.c_str()));

    ASSERT_OK(m_db->checkpoint(kCheckpointRestart, nullptr));
    ASSERT_EQ(TEST_PAGE_SIZE * 3, file_size(m_db_name.c_str()));

    ASSERT_OK(m_db->update([](auto &tx) {
        auto s = tx.drop_bucket("b");
        if (s.is_ok()) {
            s = tx.vacuum();
        }
        return s;
    }));
    ASSERT_EQ(TEST_PAGE_SIZE * 3, file_size(m_db_name.c_str()));

    // Tx::vacuum() never gets rid of the root database page, even if the whole database
    // is empty.
    ASSERT_OK(m_db->checkpoint(kCheckpointRestart, nullptr));
    ASSERT_EQ(TEST_PAGE_SIZE, file_size(m_db_name.c_str()));
}

// TEST_F(DBTests, DropBuckets)
//{
//     ASSERT_OK(m_db->update([](auto &tx) {
//         EXPECT_OK(tx.create_bucket("a", nullptr));
//         EXPECT_OK(tx.create_bucket("b", nullptr));
//         return Status::ok();
//     }));
//     ASSERT_OK(m_db->update([](auto &tx) {
//         EXPECT_OK(tx.drop_bucket("b"));
//         EXPECT_NOK(tx.drop_bucket("c"));
//         return Status::ok();
//     }));
//     ASSERT_OK(m_db->view([](const auto &tx) {
//         TestCursor a, nonexistent;
//         auto &schema = tx.schema();
//         schema.seek_first();
//         EXPECT_TRUE(schema.is_valid());
//         EXPECT_EQ("a", schema.key());
//         EXPECT_OK(test_open_bucket(tx, schema.key(), a));
//         schema.next();
//         EXPECT_FALSE(schema.is_valid());
//         EXPECT_NOK(test_open_bucket(tx, "b", nonexistent));
//         EXPECT_NOK(test_open_bucket(tx, "c", nonexistent));
//         return Status::ok();
//     }));
// }
//
// TEST_F(DBTests, RerootBuckets)
//{
//     ASSERT_OK(m_db->update([](auto &tx) {
//         EXPECT_OK(tx.create_bucket("a", nullptr));
//         EXPECT_OK(tx.create_bucket("b", nullptr));
//         EXPECT_OK(tx.create_bucket("c", nullptr));
//         EXPECT_OK(tx.create_bucket("d", nullptr));
//         reinterpret_cast<TxImpl &>(tx).TEST_validate();
//         EXPECT_OK(tx.drop_bucket("a"));
//         EXPECT_OK(tx.drop_bucket("b"));
//         EXPECT_OK(tx.drop_bucket("d"));
//         return Status::ok();
//     }));
//     ASSERT_OK(m_db->update([](auto &tx) {
//         EXPECT_OK(tx.create_bucket("e", nullptr));
//         return tx.vacuum();
//     }));
//     ASSERT_OK(m_db->view([](const auto &tx) {
//         TestBucket b, e;
//         auto &schema = tx.schema();
//         schema.seek_first();
//         EXPECT_TRUE(schema.is_valid());
//         EXPECT_EQ("c", schema.key());
//         EXPECT_OK(test_open_bucket(tx, schema.key(), c));
//         schema.next();
//         EXPECT_TRUE(schema.is_valid());
//         EXPECT_EQ("e", schema.key());
//         EXPECT_OK(test_open_bucket(tx, schema.key(), e));
//         schema.previous();
//         EXPECT_TRUE(schema.is_valid());
//         schema.next();
//         schema.next();
//         EXPECT_FALSE(schema.is_valid());
//         return Status::ok();
//     }));
// }
//
// TEST_F(DBTests, BucketExistence)
//{
//     // Cannot Tx::open_bucket() a bucket that doesn't exist, even in a read-write transaction.
//     const auto try_open_bucket = [](const auto &tx) {
//         TestBucket b;
//         return test_open_bucket(tx, "bucket", b);
//     };
//     ASSERT_NOK(m_db->view(try_open_bucket));
//     ASSERT_NOK(m_db->update(try_open_bucket));
//
//     // Tx::create_bucket() must be used for bucket creation.
//     ASSERT_OK(m_db->update([](auto &tx) {
//         return tx.create_bucket("bucket", nullptr);
//     }));
//
//     ASSERT_NOK(m_db->update([](auto &tx) {
//         BucketOptions b_opt;
//         b_opt.error_if_exists = true;
//         return tx.create_bucket(b_opt, "bucket", nullptr);
//     }));
//
//     // Now the bucket can be opened.
//     ASSERT_OK(m_db->view([](const auto &tx) {
//         TestBucket b;
//         return test_open_bucket(tx, "bucket", b);
//     }));
// }

TEST_F(DBTests, SpaceAmplification)
{
    static constexpr size_t kInputSize = 1'024 * 1'024;
    static constexpr size_t kNumRecords = kInputSize / 256;
    TEST_LOG << "DBTests.SpaceAmplification\n";

    RandomGenerator random;
    ASSERT_OK(m_db->update([&random](auto &tx) {
        TestBucket b;
        auto s = test_create_and_open_bucket(tx, "b", b);
        for (size_t i = 0; s.is_ok() && i < kNumRecords; ++i) {
            const auto key = random.Generate(kInputSize / kNumRecords / 2);
            const auto val = random.Generate(key.size());
            s = b->put(key, val);
        }
        return s;
    }));

    close_db();
    File *file;
    uint64_t file_size;
    ASSERT_OK(m_env->new_file(m_db_name.c_str(), Env::kReadOnly, file));
    ASSERT_OK(file->get_size(file_size));
    delete file;

    const auto space_amp = static_cast<double>(file_size) / static_cast<double>(kInputSize);
    TEST_LOG << "SpaceAmplification: " << space_amp << '\n';
}

// TEST_F(DBTests, VacuumDroppedBuckets)
//{
//     ASSERT_OK(m_db->update([](auto &tx) {
//         EXPECT_OK(put_range(tx, "a", 0, 1'000));
//         EXPECT_OK(put_range(tx, "b", 0, 1'000));
//         EXPECT_OK(put_range(tx, "c", 0, 1'000));
//         return Status::ok();
//     }));
//     ASSERT_OK(m_db->update([](auto &tx) {
//         TestCursor a, b, c;
//         EXPECT_OK(test_open_bucket(tx, "a", a));
//         EXPECT_OK(test_open_bucket(tx, "b", b));
//         EXPECT_OK(test_open_bucket(tx, "c", c));
//         a.reset();
//         b.reset();
//         c.reset();
//         EXPECT_OK(tx.drop_bucket("a"));
//         EXPECT_OK(tx.drop_bucket("c"));
//         return tx.vacuum();
//     }));
// }

TEST_F(DBTests, ReadWithoutWAL)
{
    const auto wal_name = m_db_name + kDefaultWalSuffix.to_string();
    ASSERT_OK(m_db->update([](auto &tx) {
        return put_range(tx, "b", 0, 1'000);
    }));
    ASSERT_TRUE(m_env->file_exists(wal_name.c_str()));

    ASSERT_OK(reopen_db(false));
    ASSERT_FALSE(m_env->file_exists(wal_name.c_str()));

    ASSERT_OK(m_db->view([](auto &tx) {
        return check_range(tx, "b", 0, 1'000, true);
    }));
    ASSERT_FALSE(m_env->file_exists(wal_name.c_str()));

    ASSERT_OK(m_db->checkpoint(kCheckpointPassive, nullptr));
    ASSERT_OK(m_db->checkpoint(kCheckpointFull, nullptr));
    ASSERT_OK(m_db->checkpoint(kCheckpointRestart, nullptr));
    ASSERT_FALSE(m_env->file_exists(wal_name.c_str()));
}

// class DBFileFormatTests : public DBTests
//{
// protected:
//     explicit DBFileFormatTests() = default;
//     ~DBFileFormatTests() override = default;
//
//     auto create_db_string()
//     {
//         static constexpr size_t kN = 1'000;
//         EXPECT_OK(reopen_db(true));
//         EXPECT_OK(m_db->update([](auto &tx) {
//             EXPECT_OK(DBTests::put_range(tx, "a", 0, kN));
//             EXPECT_OK(DBTests::put_range(tx, "b", 0, kN));
//             EXPECT_OK(DBTests::put_range(tx, "c", 0, kN));
//             EXPECT_OK(DBTests::put_range(tx, "d", 0, kN));
//
//             EXPECT_OK(tx.drop_bucket("a"));
//             EXPECT_OK(tx.vacuum());
//
//             EXPECT_OK(DBTests::erase_range(tx, "b", 0, kN / 2));
//             EXPECT_OK(DBTests::erase_range(tx, "c", kN / 4, 3 * kN / 4));
//             EXPECT_OK(DBTests::erase_range(tx, "d", kN / 2, kN));
//             return Status::ok();
//         }));
//         EXPECT_OK(m_db->checkpoint(kCheckpointRestart, nullptr));
//         return read_file_to_string(*m_env, m_db_name.c_str());
//     }
//
//     auto assert_db_strings_equal(const std::string &lhs, const std::string &rhs)
//     {
//         std::hash<std::string> hash;
//         if (hash(lhs) == hash(rhs)) {
//             return;
//         }
//         ASSERT_EQ(lhs.size(), rhs.size());
//         ASSERT_EQ(lhs.size() % TEST_PAGE_SIZE, 0);
//         ASSERT_GE(lhs.size(), TEST_PAGE_SIZE * 3);
//         const auto npages = lhs.size() / TEST_PAGE_SIZE;
//         for (size_t i = 0; i < npages; ++i) {
//             const Slice lhs_page(lhs.data() + i * TEST_PAGE_SIZE, TEST_PAGE_SIZE);
//             const Slice rhs_page(rhs.data() + i * TEST_PAGE_SIZE, TEST_PAGE_SIZE);
//             ASSERT_EQ(lhs_page, rhs_page) << "mismatch on page " << i + 1;
//         }
//     }
// };
//
// TEST_F(DBFileFormatTests, IsReproducible1)
//{
//     const auto str1 = create_db_string();
//     const auto str2 = create_db_string();
//     assert_db_strings_equal(str1, str2);
// }
//
// TEST_F(DBFileFormatTests, IsReproducible2)
//{
//     const auto str1 = create_db_string();
//     static constexpr char kJunk = '\xCC'; // 11001100
//     ASSERT_NE(kJunk, DebugAllocator::set_junk_byte(kJunk));
//     const auto str2 = create_db_string();
//     assert_db_strings_equal(str1, str2);
// }
//
// TEST(OldWalTests, HandlesOldWalFile)
//{
//     const std::string old_wal_name = get_full_filename(testing::TempDir() + "calicodb_testwal");
//     const std::string db_name = get_full_filename(testing::TempDir() + "calicodb_testdb");
//     std::filesystem::remove_all(db_name);
//     std::filesystem::remove_all(old_wal_name);
//
//     File *oldwal;
//     auto &env = default_env();
//     ASSERT_OK(env.new_file(old_wal_name.c_str(), Env::kCreate, oldwal));
//     //    ASSERT_OK(oldwal->write(42, ":3"));
//     // TODO: The above line causes this test to fail now. Need to delete old WAL files somewhere.
//     //       SQLite does it in pagerOpenWalIfPresent(), if the database is 0 bytes in size, rather
//     //       than opening the WAL. Need to figure this out!
//
//     DB *db;
//     Options options;
//     options.env = &env;
//     options.wal_filename = old_wal_name.c_str();
//     ASSERT_OK(DB::open(options, db_name.c_str(), db));
//     ASSERT_OK(db->update([](auto &tx) {
//         return tx.create_bucket("b", nullptr);
//     }));
//
//     uint64_t file_size;
//     ASSERT_OK(oldwal->get_size(file_size));
//     ASSERT_GT(file_size, options.page_size * 3);
//     delete oldwal;
//     delete db;
// }
//
// TEST(DestructionTests, DestructionBehavior)
//{
//     const auto db_name = get_full_filename(testing::TempDir() + "calicodb_test_db");
//     const auto wal_name = db_name + kDefaultWalSuffix.to_string();
//     const auto shm_name = db_name + kDefaultShmSuffix.to_string();
//     (void)default_env().remove_file(db_name.c_str());
//     (void)default_env().remove_file(wal_name.c_str());
//     (void)default_env().remove_file(shm_name.c_str());
//
//     DB *db;
//     ASSERT_OK(DB::open(Options(), db_name.c_str(), db));
//     ASSERT_OK(db->update([](auto &tx) {
//         return tx.create_bucket("b", nullptr);
//     }));
//     ASSERT_TRUE(default_env().file_exists(db_name.c_str()));
//     ASSERT_TRUE(default_env().file_exists(wal_name.c_str()));
//     ASSERT_TRUE(default_env().file_exists(shm_name.c_str()));
//
//     delete db;
//     ASSERT_TRUE(default_env().file_exists(db_name.c_str()));
//     ASSERT_FALSE(default_env().file_exists(wal_name.c_str()));
//     ASSERT_FALSE(default_env().file_exists(shm_name.c_str()));
//
//     ASSERT_OK(DB::destroy(Options(), db_name.c_str()));
//     ASSERT_FALSE(default_env().file_exists(db_name.c_str()));
//     ASSERT_FALSE(default_env().file_exists(wal_name.c_str()));
//     ASSERT_FALSE(default_env().file_exists(shm_name.c_str()));
// }
//
// TEST(DestructionTests, OnlyDeletesCalicoDatabases)
//{
//     (void)default_env().remove_file("./testdb");
//
//     // "./testdb" does not exist.
//     ASSERT_NOK(DB::destroy(Options(), "./testdb"));
//     ASSERT_FALSE(default_env().file_exists("./testdb"));
//
//     // File is too small to read the first page.
//     File *file;
//     ASSERT_OK(default_env().new_file("./testdb", Env::kCreate, file));
//     ASSERT_OK(file->write(0, "CalicoDB format"));
//     ASSERT_NOK(DB::destroy(Options(), "./testdb"));
//     ASSERT_TRUE(default_env().file_exists("./testdb"));
//
//     // Identifier is incorrect.
//     ASSERT_OK(file->write(0, "CalicoDB format 0"));
//     delete file;
//     ASSERT_NOK(DB::destroy(Options(), "./testdb"));
// }
//
// TEST(DestructionTests, OnlyDeletesCalicoWals)
//{
//     Options options;
//     options.env = &default_env();
//     options.wal_filename = "/tmp/calicodb_destruction_wal";
//
//     DB *db;
//     ASSERT_OK(DB::open(options, "/tmp/calicodb_destruction_test", db));
//     delete db;
//
//     // These files are not part of the DB.
//     File *file;
//     ASSERT_OK(options.env->new_file("/tmp/calicodb_destruction_wal_", Env::kCreate, file));
//     delete file;
//     ASSERT_OK(options.env->new_file("/tmp/calicodb_destruction_test.db", Env::kCreate, file));
//     delete file;
//
//     ASSERT_OK(DB::destroy(options, "/tmp/calicodb_destruction_test"));
//     ASSERT_TRUE(options.env->file_exists("/tmp/calicodb_destruction_wal_"));
//     ASSERT_TRUE(options.env->file_exists("/tmp/calicodb_destruction_test.db"));
// }
//
// TEST(DestructionTests, DeletesWalAndShm)
//{
//     Options options;
//     options.env = &default_env();
//
//     DB *db;
//     ASSERT_OK(DB::open(options, "./test", db));
//     delete db;
//
//     ASSERT_TRUE(options.env->file_exists("./test"));
//     ASSERT_FALSE(options.env->file_exists("./test-wal"));
//     ASSERT_FALSE(options.env->file_exists("./test-shm"));
//
//     File *file;
//     // The DB closed successfully, so the WAL and shm files were deleted. Pretend
//     // that didn't happen.
//     ASSERT_OK(options.env->new_file("./test-wal", Env::kCreate, file));
//     delete file;
//     ASSERT_OK(options.env->new_file("./test-shm", Env::kCreate, file));
//     delete file;
//
//     ASSERT_TRUE(options.env->file_exists("./test"));
//     ASSERT_TRUE(options.env->file_exists("./test-wal"));
//     ASSERT_TRUE(options.env->file_exists("./test-shm"));
//
//     ASSERT_OK(DB::destroy(options, "./test"));
//
//     ASSERT_FALSE(options.env->file_exists("./test"));
//     ASSERT_FALSE(options.env->file_exists("./test-wal"));
//     ASSERT_FALSE(options.env->file_exists("./test-shm"));
// }
//
// class DBOpenTests : public DBTests
//{
// protected:
//     ~DBOpenTests() override = default;
//
//     auto SetUp() -> void override
//     {
//         // Don't call DBTests::SetUp(). DB is opened in test body.
//     }
// };
//
// TEST_F(DBOpenTests, HandlesEmptyFilename)
//{
//     ASSERT_NOK(DB::open(Options(), "", m_db));
// }
//
// TEST_F(DBOpenTests, CreatesMissingDb)
//{
//     Options options;
//     options.error_if_exists = false;
//     options.create_if_missing = true;
//     ASSERT_OK(DB::open(options, m_db_name.c_str(), m_db));
//     delete m_db;
//     m_db = nullptr;
//
//     options.create_if_missing = false;
//     ASSERT_OK(DB::open(options, m_db_name.c_str(), m_db));
// }
//
// TEST_F(DBOpenTests, FailsIfMissingDb)
//{
//     Options options;
//     options.create_if_missing = false;
//     ASSERT_TRUE(DB::open(options, m_db_name.c_str(), m_db).is_invalid_argument());
// }
//
// TEST_F(DBOpenTests, FailsIfDbExists)
//{
//     Options options;
//     options.create_if_missing = true;
//     options.error_if_exists = true;
//     ASSERT_OK(DB::open(options, m_db_name.c_str(), m_db));
//     delete m_db;
//     m_db = nullptr;
//
//     options.create_if_missing = false;
//     ASSERT_TRUE(DB::open(options, m_db_name.c_str(), m_db).is_invalid_argument());
// }
//
// TEST_F(DBOpenTests, CustomLogger)
//{
//     class : public Logger
//     {
//     public:
//         std::string m_str;
//
//         auto append(const Slice &msg) -> void override
//         {
//             m_str.append(msg.to_string());
//         }
//
//         auto logv(const char *fmt, std::va_list args) -> void override
//         {
//             char fixed[1'024];
//             std::va_list args_copy;
//             va_copy(args_copy, args);
//
//             const auto len = std::vsnprintf(
//                 fixed, sizeof(fixed), fmt, args);
//             ASSERT_TRUE(0 <= len && len < 1'024);
//             append(fixed);
//             va_end(args_copy);
//         }
//     } logger;
//
//     Options options;
//     options.info_log = &logger;
//     // DB will warn (through the log) about the fact that options.env is not nullptr.
//     // It will clear that field and use it to hold a custom Env that helps implement
//     // in-memory databases.
//     options.env = &default_env();
//     options.temp_database = true;
//     // In-memory databases can have an empty filename.
//     ASSERT_OK(DB::open(options, "", m_db));
//     delete m_db;
//     m_db = nullptr;
//
//     ASSERT_FALSE(logger.m_str.empty());
// }
//
// TEST_F(DBOpenTests, RemembersBucketOptions)
//{
//     for (size_t i = 0; i < 2; ++i) {
//         ASSERT_OK(DB::open(Options(), m_db_name.c_str(), m_db));
//         ASSERT_OK(m_db->update([](auto &tx) {
//             BucketOptions options;
//             options.unique = false;
//             return tx.create_bucket(options, "b", nullptr);
//         }));
//         ASSERT_OK(m_db->update([i](auto &tx) {
//             TestBucket b;
//             auto s = test_open_bucket(tx, "b", b);
//             if (s.is_ok()) {
//                 s = b->put(numeric_key(i), numeric_key(1));
//             }
//             if (s.is_ok()) {
//                 s = b->put(numeric_key(i), numeric_key(0));
//             }
//             return s;
//         }));
//         delete m_db;
//     }
//     ASSERT_OK(DB::open(Options(), m_db_name.c_str(), m_db));
//     ASSERT_OK(m_db->view([](auto &tx) {
//         TestBucket b;
//         auto s = test_open_bucket(tx, "b", b);
//         if (s.is_ok()) {
//             c->find(numeric_key(0));
//             EXPECT_TRUE(c->is_valid());
//             EXPECT_EQ(c->value(), numeric_key(0));
//             c->next();
//             EXPECT_TRUE(c->is_valid());
//             EXPECT_EQ(c->value(), numeric_key(1));
//
//             c->find(numeric_key(1));
//             EXPECT_TRUE(c->is_valid());
//             EXPECT_EQ(c->value(), numeric_key(0));
//             c->next();
//             EXPECT_TRUE(c->is_valid());
//             EXPECT_EQ(c->value(), numeric_key(1));
//         }
//         return s;
//     }));
//     delete m_db;
//     m_db = nullptr;
// }
//
// class DBPageSizeTests : public DBOpenTests
//{
// public:
//     Options m_options;
//
//     explicit DBPageSizeTests()
//     {
//         m_options.wal_filename = m_alt_wal_name.c_str();
//     }
//
//     ~DBPageSizeTests() override = default;
//
//     [[nodiscard]] auto db_page_size() const -> size_t
//     {
//         return reinterpret_cast<DBImpl *>(m_db)->TEST_pager().page_size();
//     }
//
//     auto add_pages(bool commit) -> Status
//     {
//         return m_db->update([commit](auto &tx) {
//             EXPECT_OK(tx.create_bucket("a", nullptr));
//             EXPECT_OK(tx.create_bucket("b", nullptr));
//             EXPECT_OK(tx.create_bucket("c", nullptr));
//             return commit ? Status::ok() : Status::invalid_argument();
//         });
//     }
//
//     template <class Fn>
//     auto for_each_page_size(const Fn &fn) const -> void
//     {
//         for (auto ps1 = kMinPageSize; ps1 <= kMaxPageSize; ps1 *= 2) {
//             for (auto ps2 = kMinPageSize; ps2 <= kMaxPageSize; ps2 *= 2) {
//                 // Call fn(kMinPageSize, kMinPageSize) as a sanity check.
//                 if (ps1 == kMinPageSize || ps1 != ps2) {
//                     fn(ps1, ps2);
//                     (void)m_env->remove_file(m_options.wal_filename);
//                     (void)m_env->remove_file(m_db_name.c_str());
//                 }
//             }
//         }
//     }
// };
//
// TEST_F(DBPageSizeTests, EmptyDB)
//{
//     for_each_page_size([this](auto ps1, auto ps2) {
//         m_options.page_size = ps1;
//         ASSERT_OK(DB::open(m_options, m_db_name.c_str(), m_db));
//         ASSERT_NOK(add_pages(false));
//         ASSERT_EQ(db_page_size(), ps1);
//         delete exchange(m_db, nullptr);
//
//         m_options.page_size = ps2;
//         ASSERT_OK(DB::open(m_options, m_db_name.c_str(), m_db));
//         ASSERT_OK(add_pages(true));
//         ASSERT_EQ(db_page_size(), ps2);
//         delete exchange(m_db, nullptr);
//     });
// }
//
// TEST_F(DBPageSizeTests, NonEmptyDB)
//{
//     for_each_page_size([this](auto ps1, auto ps2) {
//         m_options.page_size = ps1;
//         ASSERT_OK(DB::open(m_options, m_db_name.c_str(), m_db));
//         ASSERT_OK(add_pages(true));
//         ASSERT_EQ(db_page_size(), ps1);
//         delete exchange(m_db, nullptr);
//
//         m_options.page_size = ps2;
//         ASSERT_OK(DB::open(m_options, m_db_name.c_str(), m_db));
//         ASSERT_OK(add_pages(true));
//         // Page size should be what the first connection set.
//         ASSERT_EQ(db_page_size(), ps1);
//         delete exchange(m_db, nullptr);
//     });
// }
//
// class TransactionTests : public DBTests
//{
// protected:
//     explicit TransactionTests() = default;
//
//     ~TransactionTests() override = default;
// };
//
// TEST_F(TransactionTests, ReadsMostRecentSnapshot)
//{
//     size_t key_limit = 0;
//     auto should_records_exist = false;
//     m_env->m_write_callback = [&] {
//         DB *db;
//         Options options;
//         options.page_size = TEST_PAGE_SIZE;
//         options.env = m_env;
//         auto s = DB::open(options, m_db_name.c_str(), db);
//         ASSERT_OK(s);
//         s = db->view([key_limit](auto &tx) {
//             return check_range(tx, "b", 0, key_limit, true);
//         });
//         delete db;
//         if (!should_records_exist && s.is_invalid_argument()) {
//             s = Status::ok();
//         }
//         ASSERT_OK(s);
//     };
//     ASSERT_OK(m_db->update([&](auto &tx) {
//         // WARNING: If too big of a transaction is performed, the system may run out of file
//         //          descriptors. The closing of the database file must sometimes be deferred
//         //          until after the transaction completes, since this connection will have a
//         //          write lock until DB::update() returns.
//         for (size_t i = 0; i < 10; ++i) {
//             static constexpr size_t kScale = 5;
//             EXPECT_OK(put_range(tx, "b", i * kScale, (i + 1) * kScale));
//             EXPECT_OK(tx.commit());
//             should_records_exist = true;
//             key_limit = (i + 1) * kScale;
//         }
//         return Status::ok();
//     }));
//
//     m_env->m_write_callback();
// }
//
// TEST_F(TransactionTests, ExclusiveLockingMode)
//{
//     for (int i = 0; i < 2; ++i) {
//         m_config = i == 0 ? kExclusiveLockMode : kDefault;
//         ASSERT_OK(reopen_db(false));
//         size_t n = 0;
//         m_env->m_write_callback = [this, &i, &n] {
//             if (n > 256) {
//                 return;
//             }
//             DB *db;
//             Options options;
//             options.page_size = TEST_PAGE_SIZE;
//             options.lock_mode = i == 0 ? Options::kLockNormal
//                                        : Options::kLockExclusive;
//             options.env = m_env;
//
//             Status s;
//             ASSERT_TRUE((s = DB::open(options, m_db_name.c_str(), db)).is_busy())
//                 << s.message();
//             ++n;
//         };
//         ASSERT_OK(m_db->update([](auto &tx) {
//             for (size_t i = 0; i < 50; ++i) {
//                 EXPECT_OK(put_range(tx, "b", i * 10, (i + 1) * 10));
//                 EXPECT_OK(tx.commit());
//             }
//             return Status::ok();
//         }));
//         m_env->m_write_callback = {};
//     }
// }
//
// TEST_F(TransactionTests, IgnoresFutureVersions)
//{
//     static constexpr size_t kN = 5;
//     auto has_open_db = false;
//     size_t n = 0;
//
//     ASSERT_OK(m_db->update([](auto &tx) {
//         return put_range(tx, "b", 0, kN);
//     }));
//
//     m_env->m_read_callback = [this, &has_open_db, &n] {
//         if (has_open_db || n >= kN) {
//             // Prevent this callback from being called by itself, and prevent the test from
//             // running for too long.
//             return;
//         }
//         DB *db;
//         Options options;
//         options.env = m_env;
//         options.page_size = TEST_PAGE_SIZE;
//         has_open_db = true;
//         ASSERT_OK(DB::open(options, m_db_name.c_str(), db));
//         ASSERT_OK(db->update([n](auto &tx) {
//             return put_range(tx, "b", kN * n, kN * (n + 1));
//         }));
//         delete db;
//         has_open_db = false;
//         ++n;
//     };
//
//     (void)m_db->view([&n](const auto &tx) {
//         for (size_t i = 0; i < kN; ++i) {
//             EXPECT_OK(check_range(tx, "b", 0, kN, true));
//             EXPECT_OK(check_range(tx, "b", kN, kN * (n + 1), false));
//         }
//         return Status::ok();
//     });
//
//     m_env->m_read_callback = {};
// }
//
// class CheckpointTests : public DBTests
//{
// protected:
//     explicit CheckpointTests() = default;
//
//     ~CheckpointTests() override = default;
//
//     auto SetUp() -> void override
//     {
//         ASSERT_OK(open_db(m_db));
//     }
//
//     auto open_db(DB *&db_out, BusyHandler *busy = nullptr) -> Status
//     {
//         Options options;
//         options.env = m_env;
//         options.busy = busy;
//         options.auto_checkpoint = 0;
//         options.page_size = TEST_PAGE_SIZE;
//         return DB::open(options, m_db_name.c_str(), db_out);
//     }
// };
//
// TEST_F(CheckpointTests, CheckpointerBlocksOtherCheckpointers)
//{
//     size_t n = 0;
//     for (auto mode : {kCheckpointPassive, kCheckpointFull, kCheckpointRestart}) {
//         ASSERT_OK(m_db->update([&n](auto &tx) {
//             ++n;
//             return put_range(tx, "b", (n - 1) * 1'000, n * 1'000);
//         }));
//         m_env->m_write_callback = [this] {
//             // Each time File::write() is called, use a different connection to attempt a
//             // checkpoint. It should get blocked every time, since a checkpoint is already
//             // running.
//             DB *db;
//             ASSERT_OK(open_db(db));
//             ASSERT_TRUE(db->checkpoint(kCheckpointPassive, nullptr).is_busy());
//             ASSERT_TRUE(db->checkpoint(kCheckpointFull, nullptr).is_busy());
//             ASSERT_TRUE(db->checkpoint(kCheckpointRestart, nullptr).is_busy());
//             delete db;
//         };
//         ASSERT_OK(m_db->checkpoint(mode, nullptr));
//         m_env->m_write_callback = [] {};
//     }
// }
//
// TEST_F(CheckpointTests, CheckpointerAllowsTransactions)
//{
//     static constexpr size_t kSavedCount = 100;
//
//     // Set up a DB with some records in both the database file and the WAL.
//     ASSERT_OK(m_db->update([](auto &tx) {
//         return put_range(tx, "before", 0, kSavedCount);
//     }));
//     ASSERT_OK(m_db->checkpoint(kCheckpointRestart, nullptr));
//     ASSERT_OK(m_db->update([](auto &tx) {
//         // These records will be checkpointed below. round is 1 to cause a new version of the first half of
//         // the records to be written.
//         return put_range(tx, "before", 0, kSavedCount / 2, 1);
//     }));
//
//     size_t n = 0;
//     m_env->m_write_callback = [this, &n] {
//         // NOTE: The outer DB still has the file locked, so the Env won't close the database file when
//         //       this DB is deleted. The Env implementation must reuse file descriptors, otherwise it
//         //       will likely run out during this test.
//         DB *db;
//         ASSERT_OK(open_db(db));
//         ASSERT_OK(db->update([n](auto &tx) {
//             return put_range(tx, "after", n * 2, (n + 1) * 2);
//         }));
//         (void)db->view([n](auto &tx) {
//             EXPECT_OK(check_range(tx, "before", 0, kSavedCount / 2, true, 1));
//             EXPECT_OK(check_range(tx, "before", kSavedCount / 2, kSavedCount, true, 0));
//             EXPECT_OK(check_range(tx, "after", 0, (n + 1) * 2, true));
//             return Status::ok();
//         });
//         ++n;
//         delete db;
//     };
//
//     ASSERT_OK(m_db->checkpoint(kCheckpointPassive, nullptr));
//     // Don't call the callback during close. DB::open() will return a Status::busy() due to the
//     // exclusive lock held.
//     m_env->m_write_callback = {};
// }
//
// TEST_F(CheckpointTests, CheckpointerFallsBackToLowerMode)
//{
//     size_t n = 0;
//     for (auto mode : {kCheckpointFull, kCheckpointRestart}) {
//         m_env->m_write_callback = [this, mode] {
//             DB *db;
//             class Handler : public BusyHandler
//             {
//             public:
//                 bool called = false;
//                 ~Handler() override = default;
//                 auto exec(unsigned) -> bool override
//                 {
//                     EXPECT_FALSE(called);
//                     called = true;
//                     return false;
//                 }
//             } handler;
//             ASSERT_OK(open_db(db, &handler));
//             ASSERT_TRUE(db->checkpoint(mode, nullptr).is_busy());
//             ASSERT_TRUE(handler.called);
//             delete db;
//         };
//         ASSERT_OK(m_db->update([&n](auto &tx) {
//             ++n;
//             return put_range(tx, "b", (n - 1) * 1'000, n * 1'000);
//         }));
//     }
//     m_env->m_write_callback = [] {};
// }
//
// class DBVacuumTests : public DBTests
//{
// protected:
//     explicit DBVacuumTests() = default;
//
//     ~DBVacuumTests() override = default;
//
//     auto test_configurations_impl(const std::vector<uint8_t> &bitmaps) const -> void
//     {
//         static constexpr auto *kName = "12345678_BUCKET_NAMES";
//         static constexpr size_t kN = 10;
//         (void)m_db->update([&bitmaps](auto &tx) {
//             TestCursor cursors[8];
//             for (size_t i = 0; i < 8; ++i) {
//                 EXPECT_OK(test_create_and_open_bucket(tx, kName + i, cursors[i]));
//             }
//             std::vector<size_t> bs;
//             std::vector<size_t> is;
//             for (size_t b = 0; b < bitmaps.size(); ++b) {
//                 for (size_t i = 0; i < 8; ++i) {
//                     if ((bitmaps[b] >> i) & 1) {
//                         EXPECT_OK(put_range(tx, *cursors[i], b * kN, (b + 1) * kN));
//                         bs.emplace_back(b);
//                         is.emplace_back(i);
//                     }
//                 }
//             }
//             for (size_t n = 0; n < bs.size(); ++n) {
//                 if (0 == (n & 1)) {
//                     EXPECT_OK(erase_range(tx, *cursors[is[n]], bs[n] * kN, (bs[n] + 1) * kN));
//                 }
//             }
//             EXPECT_OK(tx.vacuum());
//
//             for (size_t n = 0; n < bs.size(); ++n) {
//                 EXPECT_OK(check_range(tx, *cursors[is[n]], bs[n] * kN, (bs[n] + 1) * kN, n & 1));
//                 if (n & 1) {
//                     // Erase the rest of the records. The database should be empty after this
//                     // loop completes.
//                     EXPECT_OK(erase_range(tx, *cursors[is[n]], bs[n] * kN, (bs[n] + 1) * kN));
//                 }
//             }
//             EXPECT_OK(tx.vacuum());
//
//             for (size_t n = 0; n < bs.size(); ++n) {
//                 EXPECT_OK(check_range(tx, *cursors[is[n]], bs[n] * kN, (bs[n] + 1) * kN, false));
//             }
//             return Status::ok();
//         });
//     }
//     auto test_configurations(std::vector<uint8_t> bitmaps) const -> void
//     {
//         for (uint32_t i = 0; i < 8; ++i) {
//             for (auto &b : bitmaps) {
//                 b = static_cast<uint8_t>((b << 1) | (b >> 7));
//             }
//             test_configurations_impl(bitmaps);
//         }
//     }
// };
//
// TEST_F(DBVacuumTests, SingleBucket)
//{
//     test_configurations({
//         0b10000000,
//         0b10000000,
//         0b10000000,
//         0b10000000,
//     });
// }
//
// TEST_F(DBVacuumTests, MultipleBuckets)
//{
//     test_configurations({
//         0b10000000,
//         0b01000000,
//         0b00100000,
//         0b00010000,
//     });
//     test_configurations({
//         0b10001000,
//         0b01000100,
//         0b00100010,
//         0b00010001,
//     });
//     test_configurations({
//         0b10101000,
//         0b01010100,
//         0b00101010,
//         0b00010101,
//     });
//     test_configurations({
//         0b10101010,
//         0b01010101,
//         0b10101010,
//         0b01010101,
//     });
// }
//
// TEST_F(DBVacuumTests, SanityCheck)
//{
//     test_configurations({
//         0b11111111,
//         0b11111111,
//         0b11111111,
//         0b11111111,
//     });
// }
//
// static auto model_writer(Tx &tx) -> Status
//{
//     TestBucket b;
//     auto &schema = tx.schema();
//     schema.seek_first();
//     EXPECT_TRUE(schema.is_valid());
//     EXPECT_EQ(schema.key(), "a");
//     EXPECT_OK(test_open_bucket(tx, schema.key(), c));
//     EXPECT_OK(tx.drop_bucket(schema.key())); // Drop bucket before delete cursor.
//     c.reset();
//
//     // NOTE: The schema cursor is used to fulfill create/open/drop bucket requests. Each time
//     //       a *_bucket() method is called on the Tx object, the schema cursor may be moved.
//     schema.seek_first();
//     EXPECT_TRUE(schema.is_valid());
//     EXPECT_EQ(schema.key(), "b");
//     EXPECT_OK(test_open_bucket(tx, schema.key(), c));
//     c.reset();
//     EXPECT_OK(tx.drop_bucket(schema.key())); // Drop bucket after delete cursor.
//
//     schema.seek_first();
//     EXPECT_TRUE(schema.is_valid());
//     EXPECT_EQ(schema.key(), "c");
//     EXPECT_OK(test_open_bucket(tx, schema.key(), c));
//     c.reset();
//
//     reinterpret_cast<ModelTx &>(tx).check_consistency();
//     return Status::ok();
// }
// TEST(TestModelDB, TestModelDB)
//{
//     DB *db;
//     KVStore store;
//     Options options;
//     const auto filename = get_full_filename(testing::TempDir() + "calicodb_test_model_db");
//     remove_calicodb_files(filename);
//     ASSERT_OK(ModelDB::open(options, filename.c_str(), store, db));
//     ASSERT_OK(db->update([](auto &tx) {
//         EXPECT_OK(tx.create_bucket("a", nullptr));
//         EXPECT_OK(tx.create_bucket("b", nullptr));
//         EXPECT_OK(tx.create_bucket("c", nullptr));
//         return Status::ok();
//     }));
//     ASSERT_TRUE(default_env().file_exists(filename.c_str()));
//     ASSERT_OK(db->update(model_writer));
//     reinterpret_cast<ModelDB *>(db)->check_consistency();
//     delete db;
// }

} // namespace calicodb::test
