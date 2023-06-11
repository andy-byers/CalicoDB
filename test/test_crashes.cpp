// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/cursor.h"
#include "calicodb/db.h"
#include "calicodb/env.h"
#include "common.h"
#include "db_impl.h"
#include "logging.h"
#include "pager.h"
#include "test.h"

namespace calicodb::test
{

#define MAYBE_CRASH(target)                         \
    do {                                            \
        if ((target)->should_next_syscall_fail()) { \
            return Status::io_error("<FAULT>");     \
        }                                           \
    } while (0)

class CrashEnv : public EnvWrapper
{
public:
    mutable int m_max_num = 0;
    mutable int m_num = 0;
    bool m_enabled = false;

    explicit CrashEnv(Env &env)
        : EnvWrapper(env)
    {
    }

    ~CrashEnv() override = default;

    [[nodiscard]] auto should_next_syscall_fail() const -> bool
    {
        if (m_enabled && m_num++ >= m_max_num) {
            m_num = 0;
            ++m_max_num;
            return true;
        }
        return false;
    }

    auto remove_file(const std::string &filename) -> Status override
    {
        MAYBE_CRASH(this);
        return target()->remove_file(filename);
    }

    auto resize_file(const std::string &filename, std::size_t file_size) -> Status override
    {
        MAYBE_CRASH(this);
        return target()->resize_file(filename, file_size);
    }

    auto new_file(const std::string &filename, OpenMode mode, File *&file_out) -> Status override
    {
        MAYBE_CRASH(this);

        class CrashFile : public FileWrapper
        {
            CrashEnv *m_env;

        public:
            explicit CrashFile(CrashEnv &env, File &base)
                : FileWrapper(base),
                  m_env(&env)
            {
            }

            ~CrashFile() override
            {
                delete m_target;
            }

            auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status override
            {
                MAYBE_CRASH(m_env);
                return FileWrapper::read(offset, size, scratch, out);
            }

            auto write(std::size_t offset, const Slice &in) -> Status override
            {
                MAYBE_CRASH(m_env);
                return FileWrapper::write(offset, in);
            }

            auto sync() -> Status override
            {
                MAYBE_CRASH(m_env);
                return FileWrapper::sync();
            }

            auto file_lock(FileLockMode mode) -> Status override
            {
                MAYBE_CRASH(m_env);
                return FileWrapper::file_lock(mode);
            }

            auto shm_map(std::size_t r, bool extend, volatile void *&out) -> Status override
            {
                MAYBE_CRASH(m_env);
                return FileWrapper::shm_map(r, extend, out);
            }

            auto shm_lock(std::size_t r, std::size_t n, ShmLockFlag flags) -> Status override
            {
                if (flags & kShmLock) {
                    MAYBE_CRASH(m_env);
                }
                return FileWrapper::shm_lock(r, n, flags);
            }
        };
        auto s = target()->new_file(filename, mode, file_out);
        if (s.is_ok()) {
            file_out = new CrashFile(*this, *file_out);
        }
        return s;
    }
};

#undef MAYBE_CRASH

class TestCrashes : public testing::Test
{
protected:
    std::string m_filename;
    CrashEnv *m_env;

    explicit TestCrashes()
        : m_filename(testing::TempDir() + "crashes"),
          m_env(new CrashEnv(*Env::default_env()))
    {
    }

    ~TestCrashes() override = default;

    static constexpr std::size_t kNumRecords = 512;
    static constexpr std::size_t kNumIterations = 3;
    [[nodiscard]] static auto make_key(std::size_t n) -> Slice
    {
        static std::string s_keys[kNumRecords];
        if (s_keys[n].empty()) {
            s_keys[n] = numeric_key(n) + "::";
            // Let the keys get increasingly long so that the overflow chain code gets tested.
            s_keys[n].resize(s_keys[n].size() + n, '0');
        }
        return s_keys[n];
    }

    // Check if a status is an injected fault
    static auto is_injected_fault(const Status &s) -> bool
    {
        return s.to_string() == "I/O error: <FAULT>";
    }

    [[nodiscard]] static auto writer_task(Tx &tx, std::size_t iteration) -> Status
    {
        EXPECT_OK(tx.status());

        Status s;
        Bucket b1, b2;
        const auto name1 = std::to_string(iteration);
        const auto name2 = std::to_string((iteration + 1) % kNumIterations);

        s = tx.open_bucket(name1, b1);
        if (s.is_invalid_argument()) {
            BucketOptions options;
            options.error_if_exists = true;
            s = tx.create_bucket(options, name1, &b1);
            for (std::size_t i = 0; s.is_ok() && i < kNumRecords; ++i) {
                const auto key = make_key(i);
                s = tx.put(b1, key, key);
            }
        }
        if (!s.is_ok()) {
            EXPECT_EQ(s, tx.status());
            return s;
        }
        s = tx.create_bucket(BucketOptions(), name2, &b2);
        if (!s.is_ok()) {
            EXPECT_EQ(s, tx.status());
            return s;
        }

        auto *c = tx.new_cursor(b1);
        c->seek_first();
        for (std::size_t i = 0; i < kNumRecords; ++i) {
            if (c->is_valid()) {
                EXPECT_EQ(c->key(), make_key(i));
                EXPECT_EQ(c->key(), c->value());
                s = tx.put(b2, c->key(), c->value());

                if (!s.is_ok()) {
                    break;
                }
                c->next();
            } else {
                s = c->status();
                break;
            }
        }
        delete c;

        if (s.is_ok()) {
            s = tx.drop_bucket(name1);
        }
        if (s.is_ok()) {
            s = tx.vacuum();
        }
        EXPECT_EQ(s, tx.status());
        return s;
    }

    static auto reader_task(const Tx &tx, std::size_t iteration) -> Status
    {
        EXPECT_OK(tx.status());

        std::string b_name;
        auto &schema = tx.schema();
        schema.seek_first();
        if (schema.is_valid()) {
            b_name = schema.key().to_string();
            EXPECT_EQ(b_name, std::to_string((iteration + 1) % kNumIterations));
        } else {
            return schema.status();
        }

        Bucket b;
        auto s = tx.open_bucket(b_name, b);
        if (!s.is_ok()) {
            return s;
        }
        for (std::size_t i = 0; i < kNumRecords; ++i) {
            const auto key = make_key(i);
            std::string value;
            s = tx.get(b, key, &value);

            if (s.is_ok()) {
                EXPECT_EQ(key, value);
            } else {
                return s;
            }
        }
        auto *c = tx.new_cursor(b);
        c->seek_first();
        for (std::size_t i = 0; i < kNumRecords; ++i) {
            if (c->is_valid()) {
                EXPECT_EQ(c->key(), make_key(i));
            } else {
                s = c->status();
                break;
            }
            c->next();
        }
        EXPECT_FALSE(c->is_valid()) << "key = \"" << c->key().to_string() << '\"';
        delete c;
        return s;
    }

    auto run_until_completion(const std::function<Status()> &task) -> void
    {
        m_env->m_max_num = 0;
        Status s;
        while (is_injected_fault(s = task())) {
        }
        ASSERT_OK(s);
    }

    static auto validate(DB &db)
    {
        db_impl(&db)->TEST_pager().assert_state();
    }

    struct OperationsParameters {
        bool inject_faults = false;
        bool test_checkpoint = false;
    };
    auto run_operations_test(const OperationsParameters &param) -> void
    {
        enum SourceLocation {
            kSrcOpen,
            kSrcUpdate,
            kSrcView,
            kSrcCheckpoint,
            kNumSrcLocations,
        };
        std::size_t src_counters[kNumSrcLocations] = {};

        std::cout << "TestCrashes::Operations({\n  .inject_faults = " << std::boolalpha << param.inject_faults
                  << ",\n  .test_checkpoint = " << param.test_checkpoint << ",\n})\n\n";

        Options options;
        options.env = m_env;

        (void)DB::destroy(options, m_filename);

        for (std::size_t i = 0; i < kNumIterations; ++i) {
            m_env->m_enabled = param.inject_faults;

            DB *db;
            run_until_completion([this, &options, &db, &src_counters] {
                ++src_counters[kSrcOpen];
                auto s = DB::open(options, m_filename, db);
                if (!s.is_ok()) {
                    EXPECT_TRUE(is_injected_fault(s));
                }
                return s;
            });
            validate(*db);

            run_until_completion([i, &db, &src_counters] {
                ++src_counters[kSrcUpdate];
                return db->update([i](auto &tx) {
                    return writer_task(tx, i);
                });
            });
            validate(*db);

            run_until_completion([i, &db, &src_counters] {
                ++src_counters[kSrcView];
                return db->view([i](const auto &tx) {
                    return reader_task(tx, i);
                });
            });
            validate(*db);

            if (param.test_checkpoint) {
                run_until_completion([&db, &src_counters] {
                    ++src_counters[kSrcCheckpoint];
                    return db->checkpoint(true);
                });
            }

            m_env->m_enabled = false;
            delete db;
        }

        std::cout << " Location       | Hits per iteration\n";
        std::cout << "----------------|--------------------\n";
        std::cout << " kOpenDB        | " << std::setw(18) << static_cast<double>(src_counters[kSrcOpen]) / kNumIterations << '\n';
        std::cout << " kUpdateDB      | " << std::setw(18) << static_cast<double>(src_counters[kSrcUpdate]) / kNumIterations << '\n';
        std::cout << " kViewDB        | " << std::setw(18) << static_cast<double>(src_counters[kSrcView]) / kNumIterations << '\n';
        std::cout << " kSrcCheckpoint | " << std::setw(18) << static_cast<double>(src_counters[kSrcCheckpoint]) / kNumIterations << '\n';
        std::cout << '\n';
    }

    struct OpenCloseParameters {
        bool inject_faults = false;
        std::size_t num_iterations = 1;
    };
    auto run_open_close_test(const OpenCloseParameters &param) -> void
    {
        Options options;
        options.env = m_env;

        std::size_t tries = 0;
        for (std::size_t i = 0; i < param.num_iterations; ++i) {
            m_env->m_enabled = false;
            (void)DB::destroy(options, m_filename);

            DB *db;
            ASSERT_OK(DB::open(options, m_filename, db));
            ASSERT_OK(db->update([scale = i + 1](auto &tx) {
                Bucket b;
                auto s = tx.create_bucket(BucketOptions(), "BUCKET", &b);
                for (std::size_t i = 0; s.is_ok() && i < kNumRecords; ++i) {
                    auto kv = make_key(i).to_string();
                    kv.resize(kv.size() * scale, '0');
                    s = tx.put(b, kv, kv);
                }
                return s;
            }));

            m_env->m_enabled = param.inject_faults;
            m_env->m_max_num = static_cast<int>(i * 5);
            m_env->m_num = 0;

            delete db;

            run_until_completion([this, &options, &db, &tries] {
                ++tries;
                auto s = DB::open(options, m_filename, db);
                if (!s.is_ok()) {
                    EXPECT_TRUE(is_injected_fault(s));
                }
                return s;
            });
            validate(*db);

            delete db;
        }

        std::cout << "Tries per iteration: " << static_cast<double>(tries) / static_cast<double>(param.num_iterations) << '\n';
    }
};

TEST_F(TestCrashes, Operations)
{
    // Sanity check. No faults.
//    run_operations_test({false, false});
//    run_operations_test({false, true});

//    // Run with fault injection.
    run_operations_test({true, false});
//    run_operations_test({true, true});
}

TEST_F(TestCrashes, OpenClose)
{
    // Sanity check. No faults.
    run_open_close_test({false, 1});
    run_open_close_test({false, 2});
    run_open_close_test({false, 3});

    // Run with fault injection.
    run_open_close_test({true, 1});
    run_open_close_test({true, 2});
    run_open_close_test({true, 3});
}

} // namespace calicodb::test