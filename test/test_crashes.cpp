// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/cursor.h"
#include "calicodb/db.h"
#include "calicodb/env.h"
#include "common.h"
#include "logging.h"
#include "model.h"
#include "test.h"
#include <filesystem>

namespace calicodb::test
{

static constexpr auto *kFaultText = "<FAULT>";
static auto injected_fault() -> Status
{
    return Status::io_error(kFaultText);
}

#define MAYBE_CRASH(target)                         \
    do {                                            \
        if ((target)->should_next_syscall_fail()) { \
            return injected_fault();                \
        }                                           \
    } while (0)

class CrashEnv : public EnvWrapper
{
public:
    struct CrashState {
        int num;
        int max_num;
        bool enabled;
    };
    mutable CrashState m_oom_state = {};
    mutable CrashState m_syscall_state = {};
    bool m_drop_unsynced = false;

    explicit CrashEnv(Env &env)
        : EnvWrapper(env)
    {
    }

    ~CrashEnv() override
    {
        disable_oom_faults();
    }

    auto enable_oom_faults() -> void
    {
        Alloc::set_hook(should_next_allocation_fail, this);
        m_oom_state.enabled = true;
    }

    auto disable_oom_faults() -> void
    {
        Alloc::set_hook(nullptr, nullptr);
        m_oom_state.enabled = false;
    }

    static auto reset_counter(CrashState &state) -> void
    {
        state.max_num = 0;
        state.num = 0;
    }

    static auto advance_counter(CrashState &state) -> void
    {
        ++state.max_num;
        state.num = 0;
    }

    [[nodiscard]] auto should_next_syscall_fail() const -> bool
    {
        if (m_syscall_state.enabled &&
            m_syscall_state.num++ >= m_syscall_state.max_num) {
            return true;
        }
        return false;
    }

    [[nodiscard]] static auto should_next_allocation_fail(void *arg) -> int
    {
        if (auto *self = static_cast<CrashEnv *>(arg);
            self->m_oom_state.enabled &&
            self->m_oom_state.num++ >= self->m_oom_state.max_num) {
            return -1;
        }
        return 0;
    }

    auto remove_file(const char *filename) -> Status override
    {
        MAYBE_CRASH(this);
        return target()->remove_file(filename);
    }

    auto new_file(const char *filename, OpenMode mode, File *&file_out) -> Status override
    {
        file_out = nullptr;
        MAYBE_CRASH(this);

        class CrashFile : public FileWrapper
        {
            const std::string m_filename;
            std::string m_backup;
            CrashEnv *m_env;

            auto save_to_backup() -> void
            {
                const auto crash_state = m_env->m_syscall_state.enabled;
                m_env->m_syscall_state.enabled = false;

                size_t file_size;
                ASSERT_OK(m_env->file_size(m_filename.c_str(), file_size));
                m_backup.resize(file_size);
                ASSERT_OK(read_exact(0, file_size, m_backup.data()));

                m_env->m_syscall_state.enabled = crash_state;
            }

            auto load_from_backup() -> void
            {
                const auto crash_state = m_env->m_syscall_state.enabled;
                m_env->m_syscall_state.enabled = false;

                ASSERT_OK(resize(m_backup.size()));
                ASSERT_OK(write(0, m_backup));

                m_env->m_syscall_state.enabled = crash_state;
            }

        public:
            explicit CrashFile(CrashEnv &env, std::string filename, File &base)
                : FileWrapper(base),
                  m_filename(std::move(filename)),
                  m_env(&env)
            {
            }

            ~CrashFile() override
            {
                delete m_target;
            }

            auto read(size_t offset, size_t size, char *scratch, Slice *out) -> Status override
            {
                MAYBE_CRASH(m_env);
                return FileWrapper::read(offset, size, scratch, out);
            }

            auto write(size_t offset, const Slice &in) -> Status override
            {
                MAYBE_CRASH(m_env);
                return FileWrapper::write(offset, in);
            }

            auto sync() -> Status override
            {
                if (m_env->should_next_syscall_fail()) {
                    if (m_env->m_drop_unsynced) {
                        std::cout << "Loading "
                                  << static_cast<double>(m_backup.size()) / 1'024.0
                                  << " KiB backup\n";
                        load_from_backup();
                    }
                    return injected_fault();
                }
                auto s = FileWrapper::sync();
                if (s.is_ok() && m_env->m_drop_unsynced) {
                    save_to_backup();
                }
                return s;
            }

            auto file_lock(FileLockMode mode) -> Status override
            {
                MAYBE_CRASH(m_env);
                return FileWrapper::file_lock(mode);
            }

            auto shm_map(size_t r, bool extend, volatile void *&out) -> Status override
            {
                out = nullptr;
                MAYBE_CRASH(m_env);
                return FileWrapper::shm_map(r, extend, out);
            }

            auto shm_lock(size_t r, size_t n, ShmLockFlag flags) -> Status override
            {
                if (flags & kShmLock) {
                    MAYBE_CRASH(m_env);
                }
                return FileWrapper::shm_lock(r, n, flags);
            }
        };
        auto s = target()->new_file(filename, mode, file_out);
        if (s.is_ok()) {
            file_out = new CrashFile(*this, filename, *file_out);
        }
        return s;
    }
};

class CrashTests : public testing::Test
{
protected:
    const std::string m_filename;
    CrashEnv *m_env;
    KVStore m_store;

    explicit CrashTests()
        : m_filename(testing::TempDir() + "calicodb_crashes"),
          m_env(new CrashEnv(Env::default_env()))
    {
        auto db_name = m_filename;
        auto shm_name = m_filename + kDefaultWalSuffix.to_string();
        auto wal_name = m_filename + kDefaultShmSuffix.to_string();
        (void)m_env->remove_file(db_name.c_str());
        (void)m_env->remove_file(shm_name.c_str());
        (void)m_env->remove_file(wal_name.c_str());
    }

    ~CrashTests() override
    {
        delete m_env;
        EXPECT_EQ(Alloc::bytes_used(), 0);
    }

    static constexpr size_t kNumRecords = 64;
    static constexpr size_t kNumIterations = 2;
    [[nodiscard]] static auto make_key(size_t n) -> Slice
    {
        static std::string s_keys[kNumRecords];
        if (s_keys[n].empty()) {
            s_keys[n] = numeric_key(n) + "::";
            // Let the keys get increasingly long so that the overflow chain code gets tested.
            s_keys[n].resize(s_keys[n].size() + n * 32, '0');
        }
        return s_keys[n];
    }
    [[nodiscard]] static auto make_value(size_t n) -> std::string
    {
        return std::string(minval<size_t>(n * 6, TEST_PAGE_SIZE * 3), '*');
    }

    // Check if a status is an injected fault
    [[nodiscard]] auto is_injected_fault(const Status &s) const -> bool
    {
        if (m_env->m_oom_state.enabled) {
            return s.is_no_memory();
        }
        return s.code() == Status::kIOError &&
               0 == std::strcmp(s.message(), "<FAULT>");
    }

    [[nodiscard]] static auto writer_task(Tx &tx, size_t iteration) -> Status
    {
        EXPECT_OK(tx.status());

        Status s;
        TestCursor c1, c2;
        const auto name1 = std::to_string(iteration);
        const auto name2 = std::to_string((iteration + 1) % kNumIterations);

        s = test_open_bucket(tx, name1, c1);
        if (s.is_invalid_argument()) {
            BucketOptions options;
            options.error_if_exists = true;
            s = test_create_and_open_bucket(tx, options, name1, c1);
            if (s.is_ok()) {
                std::vector<uint32_t> keys(kNumRecords);
                std::iota(begin(keys), end(keys), 0);
                std::default_random_engine rng(42);
                std::shuffle(begin(keys), end(keys), rng);
                for (auto k : keys) {
                    s = tx.put(*c1, make_key(k), make_value(k));
                    if (!s.is_ok()) {
                        break;
                    }
                }
            }
        }
        if (!s.is_ok()) {
            if (!s.is_no_memory()) {
                EXPECT_EQ(s, tx.status());
            }
            return s;
        }
        s = test_create_and_open_bucket(tx, BucketOptions(), name2, c2);
        if (!s.is_ok()) {
            if (!s.is_no_memory()) {
                EXPECT_EQ(s, tx.status());
            }
            return s;
        }

        c1->seek_first();
        for (size_t i = 0; i < kNumRecords; ++i) {
            if (c1->is_valid()) {
                EXPECT_EQ(c1->key(), make_key(i));
                EXPECT_EQ(c1->value(), make_value(i));
                s = tx.put(*c2, c1->key(), c1->value());
                if (!s.is_ok()) {
                    break;
                }
                c1->next();
            } else {
                s = c1->status();
                break;
            }
        }
        c1.reset();
        c2.reset();

        if (s.is_ok()) {
            s = tx.drop_bucket(name1);
        }
        if (s.is_ok()) {
            s = tx.vacuum();
        }
        if (!s.is_no_memory()) { // TODO
            EXPECT_EQ(s, tx.status());
        }
        return s;
    }

    static auto reader_task(const Tx &tx, size_t iteration) -> Status
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

        TestCursor c;
        auto s = test_open_bucket(tx, b_name, c);
        if (!s.is_ok()) {
            return s;
        }
        for (size_t i = 0; i < kNumRecords; ++i) {
            const auto key = make_key(i);
            c->find(key);
            if (c->is_valid()) {
                EXPECT_EQ(c->value(), make_value(i));
            } else {
                return c->status();
            }
        }
        c->seek_first();
        for (size_t i = 0; i < kNumRecords; ++i) {
            if (c->is_valid()) {
                EXPECT_EQ(c->key(), make_key(i));
            } else {
                s = c->status();
                break;
            }
            c->next();
        }
        EXPECT_FALSE(c->is_valid()) << "key = \"" << c->key().to_string() << '\"';
        return s;
    }

    auto run_until_completion(const std::function<Status()> &task) -> void
    {
        CrashEnv::reset_counter(m_env->m_oom_state);
        CrashEnv::reset_counter(m_env->m_syscall_state);
        Status s;
        do {
            CrashEnv::advance_counter(m_env->m_oom_state);
            CrashEnv::advance_counter(m_env->m_syscall_state);
        } while (is_injected_fault(s = task()));

        ASSERT_OK(s);
        CrashEnv::reset_counter(m_env->m_oom_state);
        CrashEnv::reset_counter(m_env->m_syscall_state);
    }

    auto validate(DB &db)
    {
        auto syscall_state = std::exchange(m_env->m_syscall_state,
                                           CrashEnv::CrashState());
        auto oom_state = std::exchange(m_env->m_oom_state,
                                       CrashEnv::CrashState());

        reinterpret_cast<ModelDB &>(db).check_consistency();
        ASSERT_OK(db.run(ReadOptions(), [](const auto &tx) {
            reinterpret_cast<const ModelTx &>(tx).check_consistency();
            return tx.status();
        }));

        m_env->m_syscall_state = syscall_state;
        m_env->m_oom_state = oom_state;
    }

    enum FaultType {
        kNoFaults,
        kSyscallFaults,
        kOOMFaults,
    };

    auto hard_reset()
    {
        m_env->m_syscall_state.enabled = false;
        m_env->disable_oom_faults();
        CrashEnv::reset_counter(m_env->m_oom_state);
        CrashEnv::reset_counter(m_env->m_syscall_state);

        m_store.clear();

        // Make sure all files created during the test are unlinked.
        auto s = m_env->remove_file(m_filename.c_str());
        ASSERT_TRUE(s.is_ok() || s.is_not_found()) << s.message();
        auto filename = m_filename + kDefaultWalSuffix.to_string();
        s = m_env->remove_file(filename.c_str());
        ASSERT_TRUE(s.is_ok() || s.is_not_found()) << s.message();
        filename = m_filename + kDefaultShmSuffix.to_string();
        s = m_env->remove_file(filename.c_str());
        ASSERT_TRUE(s.is_ok() || s.is_not_found()) << s.message();
    }

    auto set_fault_injection_type(FaultType type)
    {
        switch (type) {
            case kNoFaults:
                break;
            case kSyscallFaults:
                m_env->m_syscall_state.enabled = true;
                break;
            case kOOMFaults:
                m_env->enable_oom_faults();
                break;
            default:
                ADD_FAILURE() << "unrecognized fault type " << static_cast<int>(type);
        }
    }

    static auto get_fault_injection_state(CrashEnv &env, FaultType type) -> CrashEnv::CrashState *
    {
        switch (type) {
            case kSyscallFaults:
                return &env.m_syscall_state;
            case kOOMFaults:
                return &env.m_oom_state;
            default:
                if (type != kNoFaults) {
                    ADD_FAILURE() << "unrecognized fault type " << static_cast<int>(type);
                }
                return nullptr;
        }
    }

    struct OperationsParameters {
        FaultType fault_type = kNoFaults;
        bool auto_checkpoint = false;
        bool test_sync_mode = false;
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
        size_t src_counters[kNumSrcLocations] = {};

        const char *fault_type_str;
        if (param.fault_type == kSyscallFaults) {
            fault_type_str = "kSyscallFaults";
        } else if (param.fault_type == kOOMFaults) {
            fault_type_str = "kOOMFaults";
        } else {
            fault_type_str = "kNoFaults";
        }
        std::cout << "CrashTests::Operations({\n  .fault_type = " << fault_type_str
                  << ",\n  .auto_checkpoint = " << std::boolalpha << param.auto_checkpoint
                  << ",\n})\n\n";

        Options options;
        options.env = m_env;
        options.auto_checkpoint = param.auto_checkpoint ? 500 : 0;
        options.sync_mode = param.test_sync_mode
                                ? Options::kSyncFull
                                : Options::kSyncNormal;
        // m_drop_unsynced has no effect unless m_syscall_state.enabled is true. If both are true, then failures on fsync()
        // cause all data written since the last fsync() to be dropped. This only applies to the file that encountered
        // the fault.
        m_env->m_drop_unsynced = param.test_sync_mode;

        for (size_t i = 0; i < kNumIterations; ++i) {
            set_fault_injection_type(param.fault_type);

            run_until_completion([this, i, &options, &src_counters] {
                UserPtr<DB> db;
                ++src_counters[kSrcOpen];
                auto s = ModelDB::open(options, m_filename.c_str(), m_store, db.ref());
                if (!s.is_ok()) {
                    EXPECT_TRUE(is_injected_fault(s));
                    return s;
                }
                validate(*db);

                ++src_counters[kSrcUpdate];
                s = db->run(WriteOptions(), [i](auto &tx) {
                    return writer_task(tx, i);
                });
                if (!s.is_ok()) {
                    EXPECT_TRUE(is_injected_fault(s));
                    return s;
                }
                validate(*db);

                ++src_counters[kSrcView];
                s = db->run(ReadOptions(), [i](const auto &tx) {
                    return reader_task(tx, i);
                });
                if (!s.is_ok()) {
                    EXPECT_TRUE(is_injected_fault(s));
                    return s;
                }
                validate(*db);

                ++src_counters[kSrcCheckpoint];
                s = db->checkpoint(true);
                if (!s.is_ok()) {
                    EXPECT_TRUE(is_injected_fault(s));
                    return s;
                }
                CALICODB_EXPECT_TRUE(db);
                validate(*db);
                return s;
            });
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
        FaultType fault_type = kNoFaults;
        size_t num_iterations = 1;
    };
    auto run_open_close_test(const OpenCloseParameters &param) -> void
    {
        Options options;
        options.env = m_env;

        size_t tries = 0;
        for (size_t i = 0; i < param.num_iterations; ++i) {
            hard_reset();

            DB *db;
            ASSERT_OK(ModelDB::open(options, m_filename.c_str(), m_store, db));

            set_fault_injection_type(param.fault_type);
            if (auto *state = get_fault_injection_state(*m_env, param.fault_type)) {
                state->max_num = static_cast<int>(i * 5);
                state->num = 0;
            }

            delete db;

            run_until_completion([this, &options, &db, &tries] {
                ++tries;
                auto s = ModelDB::open(options, m_filename.c_str(), m_store, db);
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

    struct CursorReadParameters {
        FaultType fault_type = kNoFaults;
    };
    auto run_cursor_read_test(const CursorReadParameters &param) -> void
    {
        Options options;
        options.env = m_env;
        options.sync_mode = Options::kSyncFull;

        for (size_t i = 0; i < kNumIterations; ++i) {
            hard_reset();

            DB *db;
            ASSERT_OK(ModelDB::open(options, m_filename.c_str(), m_store, db));
            ASSERT_OK(db->run(WriteOptions(), [](auto &tx) {
                TestCursor c, keep_open;
                auto s = test_create_and_open_bucket(tx, BucketOptions(), "BUCKET", c);
                if (!s.is_ok()) {
                    return s;
                }
                if (s.is_ok()) {
                    s = test_open_bucket(tx, "BUCKET", keep_open);
                }
                if (!s.is_ok()) {
                    return s;
                }
                keep_open->seek_first();
                for (size_t j = 0; s.is_ok() && j < kNumRecords; ++j) {
                    s = tx.put(*c, make_key(j), make_value(j));
                }
                if (!c->status().is_ok()) {
                    const auto before_status = c->status();
                    EXPECT_NOK(before_status);
                    keep_open->seek_first();
                    EXPECT_EQ(before_status, c->status());
                }
                return s;
            }));

            set_fault_injection_type(param.fault_type);
            run_until_completion([&db] {
                return db->run(ReadOptions(), [](const auto &tx) {
                    TestCursor c;
                    auto s = test_open_bucket(tx, "BUCKET", c);
                    if (!s.is_ok()) {
                        return s;
                    }
                    c->seek_first();
                    for (size_t j = 0; c->is_valid() && j < kNumRecords; ++j) {
                        EXPECT_EQ(c->key(), make_key(j));
                        EXPECT_EQ(c->value(), make_value(j));
                        c->next();
                    }
                    s = c->status();

                    // If there was an error, this call will clear it and try again. If the pager
                    // is able to get the necessary pages without encountering another error, we
                    // can proceed with scanning the bucket.
                    c->seek_last();
                    for (size_t j = 0; c->is_valid() && j < kNumRecords; ++j) {
                        EXPECT_EQ(c->key(), make_key(kNumRecords - j - 1));
                        EXPECT_EQ(c->value(), make_value(kNumRecords - j - 1));
                        c->previous();
                    }
                    if (s.is_ok()) {
                        s = c->status();
                    }

                    for (size_t j = 0; j < kNumRecords; ++j) {
                        c->seek(make_key(j));
                        if (!c->is_valid()) {
                            break;
                        }
                        EXPECT_EQ(c->key(), make_key(j));
                        EXPECT_EQ(c->value(), make_value(j));
                    }
                    if (s.is_ok()) {
                        s = c->status();
                    }
                    return s;
                });
            });

            delete db;
        }
    }

    auto run_cursor_modify_test(const OperationsParameters &param) -> void
    {
        Options options;
        options.env = m_env;
        options.sync_mode = param.test_sync_mode
                                ? Options::kSyncFull
                                : Options::kSyncNormal;
        // m_drop_unsynced has no effect unless m_syscall_state.enabled is true. If both are true, then failures on fsync()
        // cause all data written since the last fsync() to be dropped. This only applies to the file that encountered
        // the fault.
        m_env->m_drop_unsynced = param.test_sync_mode;

        for (size_t i = 0; i < kNumIterations; ++i) {
            set_fault_injection_type(param.fault_type);

            DB *db;
            run_until_completion([this, &options, &db] {
                auto s = ModelDB::open(options, m_filename.c_str(), m_store, db);
                if (!s.is_ok()) {
                    EXPECT_TRUE(is_injected_fault(s));
                }
                return s;
            });
            validate(*db);

            run_until_completion([&db] {
                return db->run(WriteOptions(), [](auto &tx) {
                    TestCursor c;
                    auto s = test_create_and_open_bucket(tx, BucketOptions(), "BUCKET", c);
                    for (size_t j = 0; s.is_ok() && j < kNumRecords; ++j) {
                        const auto key = make_key(j);
                        const auto value = make_value(j);
                        s = tx.put(*c, key, value);
                        if (s.is_ok()) {
                            EXPECT_TRUE(c->is_valid());
                            EXPECT_EQ(c->key(), key);
                            EXPECT_EQ(c->value(), value);
                        }
                    }
                    if (s.is_ok()) {
                        c->seek_last();
                        s = c->status();
                    }
                    while (s.is_ok() && c->is_valid()) {
                        auto v = c->value().to_string();
                        v.append(v);
                        s = tx.put(*c, c->key(), v);
                        if (s.is_ok()) {
                            EXPECT_TRUE(c->is_valid());
                            EXPECT_EQ(c->value(), v);
                            c->previous();
                        }
                    }
                    if (s.is_ok()) {
                        c->seek_first();
                        s = c->status();
                    }
                    while (s.is_ok() && c->is_valid()) {
                        s = tx.erase(*c);
                    }
                    return s;
                });
            });
            validate(*db);
            delete db;
        }
    }
};

TEST_F(CrashTests, Operations_None)
{
    // Sanity check. No faults.
    run_operations_test({kNoFaults, false});
    run_operations_test({kNoFaults, true});
}

TEST_F(CrashTests, Operations_Syscall)
{
    // Run with syscall fault injection.
    run_operations_test({kSyscallFaults, false, false});
    run_operations_test({kSyscallFaults, true, false});
    run_operations_test({kSyscallFaults, false, true});
    run_operations_test({kSyscallFaults, true, true});
}

TEST_F(CrashTests, Operations_OOM)
{
    // Run with OOM fault injection.
    run_operations_test({kOOMFaults, false, false});
    run_operations_test({kOOMFaults, true, false});
    run_operations_test({kOOMFaults, false, true});
    run_operations_test({kOOMFaults, true, true});
}

TEST_F(CrashTests, OpenClose_None)
{
    run_open_close_test({kNoFaults, 3});
}

TEST_F(CrashTests, OpenClose_Syscall)
{
    run_open_close_test({kSyscallFaults, 3});
}

TEST_F(CrashTests, OpenClose_OOM)
{
    run_open_close_test({kOOMFaults, 3});
}

TEST_F(CrashTests, CursorRead_None)
{
    run_cursor_read_test({kNoFaults});
}

TEST_F(CrashTests, CursorRead_Syscall)
{
    run_cursor_read_test({kSyscallFaults});
}

TEST_F(CrashTests, CursorRead_OOM)
{
    run_cursor_read_test({kOOMFaults});
}

TEST_F(CrashTests, CursorModification_None)
{
    run_cursor_modify_test({kNoFaults, false});
    run_cursor_modify_test({kNoFaults, true});
}

TEST_F(CrashTests, CursorModification_Syscall)
{
    run_cursor_modify_test({kSyscallFaults, false, false});
    run_cursor_modify_test({kSyscallFaults, true, false});
    run_cursor_modify_test({kSyscallFaults, false, true});
    run_cursor_modify_test({kSyscallFaults, true, true});
}

TEST_F(CrashTests, CursorModification_OOM)
{
    run_cursor_modify_test({kOOMFaults, false, false});
    run_cursor_modify_test({kOOMFaults, true, false});
    run_cursor_modify_test({kOOMFaults, false, true});
    run_cursor_modify_test({kOOMFaults, true, true});
}

#undef MAYBE_CRASH

class DataLossEnv : public EnvWrapper
{
    class DataLossFile : public FileWrapper
    {
        friend class DataLossEnv;

        const std::string m_filename;
        DataLossEnv *m_env;

    public:
        explicit DataLossFile(DataLossEnv &env, std::string filename, File &base)
            : FileWrapper(base),
              m_filename(std::move(filename)),
              m_env(&env)
        {
        }

        ~DataLossFile() override
        {
            (void)sync_impl();
            delete m_target;
        }

        auto initialize_buffer(size_t file_size) -> void
        {
            m_env->m_buffer.resize(file_size);
            EXPECT_OK(FileWrapper::read(0, file_size, m_env->m_buffer.data(), nullptr));
        }

        auto perform_writes() -> void
        {
            std::uniform_int_distribution dist;
            const Slice buffer(m_env->m_buffer);
            const auto &wr = m_env->m_writes;
            const auto loss_type = m_env->m_loss_type;
            auto itr = cbegin(wr);
            for (size_t i = 0; i < wr.size(); ++i, ++itr) {
                auto [ofs, len] = *itr;
                if ((loss_type == kLoseEarly && i < wr.size() / 2) ||
                    (loss_type == kLoseLate && i >= wr.size() / 2) ||
                    (loss_type == kLoseRandom && dist(m_env->m_rng) % 4 == 0) ||
                    loss_type == kLoseAll) {
                    m_env->m_dropped_bytes += len;
                    continue;
                }
                ASSERT_OK(FileWrapper::write(ofs, buffer.range(ofs, len)));
            }
            // Refresh the buffer.
            EXPECT_OK(FileWrapper::read(0, m_env->m_buffer.size(), m_env->m_buffer.data(), nullptr));
        }

        auto write(size_t offset, const Slice &data) -> Status override
        {
            EXPECT_FALSE(data.is_empty());
            if (m_filename != m_env->m_drop_file) {
                return FileWrapper::write(offset, data);
            }
            if (m_env->m_buffer.size() < offset + data.size()) {
                m_env->m_buffer.resize(offset + data.size());
            }
            std::memcpy(m_env->m_buffer.data() + offset, data.data(), data.size());
            const auto itr = m_env->m_writes.find(offset);
            if (itr == end(m_env->m_writes)) {
                m_env->m_writes.insert(itr, {offset, data.size()});
            } else {
                // Writes must never overlap.
                EXPECT_EQ(itr->second, data.size());
            }
            return Status::ok();
        }

        auto read(size_t offset, size_t length, char *scratch, Slice *slice_out) -> Status override
        {
            if (m_filename != m_env->m_drop_file) {
                return FileWrapper::read(offset, length, scratch, slice_out);
            }
            size_t len = 0;
            if (offset < m_env->m_buffer.size()) {
                len = minval(length, m_env->m_buffer.size() - offset);
                std::memcpy(scratch, m_env->m_buffer.data() + offset, len);
            }
            if (slice_out) {
                *slice_out = Slice(scratch, len);
            }
            return Status::ok();
        }

        auto resize(size_t size) -> Status override
        {
            m_env->m_buffer.resize(size);
            m_env->m_writes.erase(m_env->m_writes.lower_bound(size),
                                  end(m_env->m_writes));
            return FileWrapper::resize(size);
        }

        auto sync() -> Status override
        {
            return sync_impl();
        }

        auto sync_impl() -> Status
        {
            if (m_filename == m_env->m_drop_file) {
                // If there were any dropped writes, sync() must return an error, otherwise the database
                // cannot figure out that something has gone wrong. It'll likely show up as corruption
                // later on.
                perform_writes();
                return injected_fault();
            }
            return Status::ok();
        }
    };

public:
    std::default_random_engine m_rng;
    enum DataLossType {
        kLoseRandom,
        kLoseEarly,
        kLoseLate,
        kLoseAll,
        kTypeCount,
    } m_loss_type;
    std::map<size_t, size_t> m_writes;
    std::string m_buffer;
    std::string m_drop_file;
    size_t m_dropped_bytes = 0;

    explicit DataLossEnv(Env &env)
        : EnvWrapper(env),
          m_rng(42)
    {
    }

    ~DataLossEnv() override = default;

    // WARNING: m_drop_file must be set before this method is called
    auto new_file(const char *filename, OpenMode mode, File *&file_out) -> Status override
    {
        auto s = target()->new_file(filename, mode, file_out);
        if (s.is_ok()) {
            auto *file = new DataLossFile(*this, filename, *file_out);
            file_out = file;
            if (filename == m_drop_file) {
                size_t file_size;
                EXPECT_OK(EnvWrapper::file_size(filename, file_size));
                file->initialize_buffer(file_size);
            }
        }
        return s;
    }
};

TEST(DataLossEnvTests, NormalOperations)
{
    const auto filename = testing::TempDir() + "calicodb_data_loss_env_tests";
    DataLossEnv env(Env::default_env());
    (void)env.remove_file(filename.c_str());

    File *file;
    ASSERT_OK(env.new_file(filename.c_str(), Env::kCreate | Env::kReadWrite, file));

    ASSERT_OK(file->write(0, "0123"));
    ASSERT_OK(file->write(4, "4567"));
    ASSERT_OK(file->write(8, "8901"));

    char buffer[4];
    for (int i = 0; i < 2; ++i) {
        ASSERT_OK(file->read_exact(0, 4, buffer));
        ASSERT_EQ(Slice(buffer, 4), "0123");
        ASSERT_OK(file->read_exact(4, 4, buffer));
        ASSERT_EQ(Slice(buffer, 4), "4567");
        ASSERT_OK(file->read_exact(8, 4, buffer));
        ASSERT_EQ(Slice(buffer, 4), "8901");
        ASSERT_OK(file->sync());
    }
    delete file;
    ASSERT_OK(env.remove_file(filename.c_str()));
}

TEST(DataLossEnvTests, DroppedWrites)
{
    const auto filename = testing::TempDir() + "calicodb_data_loss_env_tests";
    DataLossEnv env(Env::default_env());
    (void)env.remove_file(filename.c_str());

    env.m_drop_file = filename;
    env.m_loss_type = DataLossEnv::kLoseEarly;

    File *file;
    ASSERT_OK(env.new_file(filename.c_str(), Env::kCreate | Env::kReadWrite, file));

    ASSERT_OK(file->write(0, "0123"));
    ASSERT_OK(file->write(4, "4567"));
    ASSERT_OK(file->write(8, "8901"));

    char buffer[4];
    for (int i = 0; i < 2; ++i) {
        ASSERT_OK(file->read_exact(0, 4, buffer));
        if (i == 0) {
            // The first round of reads comes out of the "page cache" (m_buffer member
            // of DataLossFile).
            ASSERT_EQ(Slice(buffer, 4), "0123");
        } else {
            // Further reads should not see the first write.
            ASSERT_EQ(Slice(buffer, 4), Slice("\0\0\0\0", 4));
        }
        ASSERT_OK(file->read_exact(4, 4, buffer));
        ASSERT_EQ(Slice(buffer, 4), "4567");
        ASSERT_OK(file->read_exact(8, 4, buffer));
        ASSERT_EQ(Slice(buffer, 4), "8901");

        // This is where the data is actually dropped.
        ASSERT_NOK(file->sync());
    }

    ASSERT_OK(file->write(0, "0123"));
    ASSERT_OK(file->write(4, "0000"));
    ASSERT_OK(file->write(8, "0000"));
    ASSERT_OK(file->write(12, "0000"));
    ASSERT_OK(file->write(16, "0000"));
    // Gets rid of all writes except for the first 2.
    ASSERT_OK(file->resize(8));
    // Gets rid of the second write.
    env.m_loss_type = DataLossEnv::kLoseLate;
    ASSERT_NOK(file->sync());

    ASSERT_OK(file->read_exact(0, 4, buffer));
    ASSERT_EQ(Slice(buffer, 4), "0123");
    ASSERT_OK(file->read_exact(4, 4, buffer));
    ASSERT_EQ(Slice(buffer, 4), "4567"); // Reverted
    Slice read;
    ASSERT_OK(file->read(8, 4, buffer, &read));
    ASSERT_TRUE(read.is_empty());
    ASSERT_OK(file->read(12, 4, buffer, &read));
    ASSERT_TRUE(read.is_empty());

    delete file;
    ASSERT_OK(env.remove_file(filename.c_str()));
}

class DataLossTests : public testing::Test
{
public:
    const std::string m_filename;
    DataLossEnv *m_env = nullptr;
    DB *m_db = nullptr;

    explicit DataLossTests()
        : m_filename(testing::TempDir() + "calicodb_dropped_writes")
    {
    }

    ~DataLossTests() override
    {
        delete m_db;
        delete m_env;
    }

    auto reopen_db(bool clear = false) -> void
    {
        delete m_db;
        m_db = nullptr;
        if (clear) {
            std::filesystem::remove_all(m_filename);
            std::filesystem::remove_all(m_filename + kDefaultWalSuffix.to_string());
            std::filesystem::remove_all(m_filename + kDefaultShmSuffix.to_string());
        }
        delete m_env;
        m_env = new DataLossEnv(Env::default_env());

        Options options;
        options.env = m_env;
        options.auto_checkpoint = 0;
        options.sync_mode = Options::kSyncFull;
        ASSERT_OK(DB::open(options, m_filename.c_str(), m_db));
    }

    struct DropParameters {
        DataLossEnv::DataLossType loss_type;
        std::string loss_file;
    };
    auto perform_writes(const DropParameters &param, size_t num_writes, size_t version)
    {
        // Don't drop any records until the commit.
        m_env->m_drop_file = "";
        return m_db->run(WriteOptions(), [num_writes, version, &param, this](auto &tx) {
            TestCursor c;
            EXPECT_OK(test_create_and_open_bucket(tx, BucketOptions(), "bucket", c));
            for (size_t i = 0; i < num_writes; ++i) {
                EXPECT_OK(tx.put(*c, numeric_key(i), numeric_key(i + version * num_writes)));
            }
            m_env->m_loss_type = param.loss_type;
            m_env->m_drop_file = param.loss_file;
            auto s = tx.commit();
            if (!s.is_ok()) {
                EXPECT_EQ(injected_fault(), s);
                EXPECT_EQ(injected_fault(), tx.commit());
            }
            return s;
        });
    }

    auto perform_checkpoint(const DropParameters &param, bool reset)
    {
        m_env->m_loss_type = param.loss_type;
        m_env->m_drop_file = param.loss_file;
        auto s = m_db->checkpoint(reset);
        if (!s.is_ok()) {
            EXPECT_EQ(injected_fault(), s);
            EXPECT_EQ(injected_fault(), m_db->checkpoint(reset));
        }
        return s;
    }

    auto check_records(size_t num_writes, size_t version)
    {
        return m_db->run(ReadOptions(), [=](const auto &tx) {
            TestCursor c;
            auto s = test_open_bucket(tx, "bucket", c);
            for (size_t i = 0; i < num_writes && s.is_ok(); ++i) {
                c->find(numeric_key(i));
                if (c->is_valid()) {
                    EXPECT_EQ(c->value(), numeric_key(i + version * num_writes));
                }
            }
            return s;
        });
    }

    static auto loss_type_to_string(DataLossEnv::DataLossType type)
    {
        switch (type) {
            case DataLossEnv::kLoseRandom:
                return "kLoseRandom";
            case DataLossEnv::kLoseEarly:
                return "kLoseEarly";
            case DataLossEnv::kLoseLate:
                return "kLoseLate";
            case DataLossEnv::kLoseAll:
                return "kLoseAll";
            default:
                return "<unrecognized>";
        }
    }

    auto run_transaction_test(DataLossEnv::DataLossType loss_type, bool reopen_after_failure)
    {
        reopen_db(true);

        static constexpr size_t kNumWrites = 1'000;
        ASSERT_OK(perform_writes({}, kNumWrites, 0));

        // Only the WAL is written during a transaction.
        const DropParameters drop_param = {loss_type, m_filename + kDefaultWalSuffix.to_string()};

        ASSERT_EQ(injected_fault(), perform_writes(drop_param, kNumWrites, 1));
        ASSERT_OK(check_records(kNumWrites, 0));

        if (reopen_after_failure) {
            const auto temp = m_env->m_dropped_bytes;
            reopen_db(false);
            m_env->m_dropped_bytes = temp;
        }

        ASSERT_OK(perform_checkpoint({}, true));
        ASSERT_OK(check_records(kNumWrites, 0));
        ASSERT_OK(perform_writes({}, kNumWrites, 1));
        ASSERT_OK(check_records(kNumWrites, 1));
    }

    auto run_checkpoint_test(DataLossEnv::DataLossType loss_type, bool reopen_after_failure)
    {
        reopen_db(true);

        static constexpr size_t kNumWrites = 1'000;
        ASSERT_OK(perform_writes({}, kNumWrites, 0));
        ASSERT_OK(perform_checkpoint({}, true));

        const DropParameters drop_param = {loss_type, m_filename};

        ASSERT_OK(perform_writes({}, kNumWrites, 1));
        ASSERT_EQ(injected_fault(), perform_checkpoint(drop_param, true));
        // Any records contained in the pages being checkpointed should continue being read from
        // the WAL: the backfill count was not increased due to the failed call to File::sync().
        ASSERT_OK(check_records(kNumWrites, 1));

        if (reopen_after_failure) {
            const auto temp = m_env->m_dropped_bytes;
            reopen_db(false);
            m_env->m_dropped_bytes = temp;
        }

        ASSERT_OK(perform_writes({}, kNumWrites, 2));
        ASSERT_OK(perform_checkpoint({}, true));
        ASSERT_OK(check_records(kNumWrites, 2));
    }

    auto run_test(void (DataLossTests::*cb)(DataLossEnv::DataLossType, bool))
    {
        for (DataLossEnv::DataLossType loss_type = DataLossEnv::kLoseRandom;
             loss_type < DataLossEnv::kTypeCount;
             loss_type = static_cast<DataLossEnv::DataLossType>(loss_type + 1)) {
            for (int i = 0; i < 2; ++i) {
                const auto reopen_after_failure = i == 1;
                (this->*cb)(loss_type, reopen_after_failure);
                std::cout << loss_type_to_string(loss_type)
                          << (reopen_after_failure ? " (reopen)" : "")
                          << ": dropped " << m_env->m_dropped_bytes << " bytes\n";
                m_env->m_dropped_bytes = 0;
            }
        }
    }
};

TEST_F(DataLossTests, Transactions)
{
    run_test(&DataLossTests::run_transaction_test);
}

TEST_F(DataLossTests, Checkpoints)
{
    run_test(&DataLossTests::run_checkpoint_test);
}

} // namespace calicodb::test