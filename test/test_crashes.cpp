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

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

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

    explicit CrashEnv(Env &env)
        : EnvWrapper(env)
    {
    }

    ~CrashEnv() override
    {
        disable_oom_faults();
    }

    void enable_oom_faults()
    {
        DebugAllocator::set_hook(should_next_allocation_fail, this);
        m_oom_state.enabled = true;
    }

    void disable_oom_faults()
    {
        DebugAllocator::set_hook(nullptr, nullptr);
        m_oom_state.enabled = false;
    }

    static void reset_counter(CrashState &state)
    {
        state.max_num = 0;
        state.num = 0;
    }

    static void advance_counter(CrashState &state)
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

            void save_to_backup()
            {
                const auto crash_state = m_env->m_syscall_state.enabled;
                m_env->m_syscall_state.enabled = false;

                uint64_t file_size;
                ASSERT_OK(get_size(file_size));
                m_backup.resize(file_size);
                ASSERT_OK(read_exact(0, file_size, m_backup.data()));

                m_env->m_syscall_state.enabled = crash_state;
            }

            void load_from_backup()
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

            auto read(uint64_t offset, size_t size, char *scratch, Slice *out) -> Status override
            {
                MAYBE_CRASH(m_env);
                return FileWrapper::read(offset, size, scratch, out);
            }

            auto write(uint64_t offset, const Slice &in) -> Status override
            {
                MAYBE_CRASH(m_env);
                return FileWrapper::write(offset, in);
            }

            auto sync() -> Status override
            {
                if (m_env->should_next_syscall_fail()) {
                    return injected_fault();
                }
                return FileWrapper::sync();
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
    ModelStore m_store;

    explicit CrashTests()
        : m_filename(get_full_filename(testing::TempDir() + "calicodb_crashes")),
          m_env(new CrashEnv(default_env()))
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
        BucketPtr b1, b2;
        const auto name1 = std::to_string(iteration);
        const auto name2 = std::to_string((iteration + 1) % kNumIterations);

        s = test_open_bucket(tx, name1, b1);
        if (s.is_invalid_argument()) {
            s = test_create_bucket_if_missing(tx, name1, b1);
            if (s.is_ok()) {
                std::vector<uint32_t> keys(kNumRecords);
                std::iota(begin(keys), end(keys), 0);
                std::default_random_engine rng(42);
                std::shuffle(begin(keys), end(keys), rng);
                for (auto k : keys) {
                    const auto v = make_value(k);
                    s = b1->put(make_key(k), v);
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
        s = test_create_bucket_if_missing(tx, name2, b2);
        if (!s.is_ok()) {
            if (!s.is_no_memory()) {
                EXPECT_EQ(s, tx.status());
            }
            return s;
        }

        auto c1 = test_new_cursor(*b1);
        auto c2 = test_new_cursor(*b2);
        if (!c1 || !c2) {
            return Status::no_memory();
        }
        c1->seek_first();
        for (size_t i = 0; i < kNumRecords; ++i) {
            if (c1->is_valid()) {
                EXPECT_EQ(c1->key(), make_key(i));
                EXPECT_EQ(c1->value(), make_value(i));
                s = b2->put(c1->key(), c1->value());
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
            s = tx.main_bucket().drop_bucket(name1);
        }
        if (s.is_ok()) {
            s = tx.vacuum();
        }
        if (!s.is_no_memory()) {
            EXPECT_EQ(s, tx.status());
        }
        return s;
    }

    static auto reader_task(const Tx &tx, size_t iteration) -> Status
    {
        EXPECT_OK(tx.status());

        std::string b_name;
        auto toplevel = test_new_cursor(tx.main_bucket());
        if (!toplevel) {
            return Status::no_memory();
        }
        toplevel->seek_first();
        if (toplevel->is_valid()) {
            b_name = toplevel->key().to_string();
            EXPECT_EQ(b_name, std::to_string((iteration + 1) % kNumIterations));
        } else {
            return toplevel->status();
        }

        BucketPtr b;
        auto s = test_open_bucket(tx, b_name, b);
        if (!s.is_ok()) {
            return s;
        }
        auto c = test_new_cursor(*b);
        if (!c) {
            return Status::no_memory();
        }
        for (size_t i = 0; i < kNumRecords; ++i) {
            const auto key = make_key(i);
            c->find(key);
            if (c->is_valid()) {
                EXPECT_EQ(c->value().to_string(), make_value(i));
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

    void run_until_completion(const std::function<Status()> &task)
    {
        CrashEnv::reset_counter(m_env->m_oom_state);
        CrashEnv::reset_counter(m_env->m_syscall_state);
        Status s;
        while (is_injected_fault(s = task())) {
            CrashEnv::advance_counter(m_env->m_oom_state);
            CrashEnv::advance_counter(m_env->m_syscall_state);
        }

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
        ASSERT_OK(db.view([](const auto &tx) {
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

        m_store.tree.clear();

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
        bool in_memory = false;
    };
    void run_operations_test(const OperationsParameters &param)
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
        TEST_LOG << "CrashTests::Operations({\n  .fault_type = " << fault_type_str
                 << ",\n  .auto_checkpoint = " << std::boolalpha << param.auto_checkpoint
                 << ",\n})\n\n";

        Options options;
        options.env = m_env;
        options.create_if_missing = true;
        options.temp_database = param.in_memory;
        options.auto_checkpoint = param.auto_checkpoint ? 500 : 0;
        options.sync_mode = param.test_sync_mode
                                ? Options::kSyncFull
                                : Options::kSyncNormal;

        for (size_t i = 0; i < kNumIterations; ++i) {
            set_fault_injection_type(param.fault_type);

            run_until_completion([this, i, &options, &src_counters] {
                if (options.temp_database) {
                    m_store.tree.clear();
                }
                UserPtr<DB> db;
                ++src_counters[kSrcOpen];
                auto s = ModelDB::open(options, m_filename.c_str(), m_store, db.ref());
                if (!s.is_ok()) {
                    EXPECT_TRUE(is_injected_fault(s));
                    return s;
                }
                validate(*db);

                ++src_counters[kSrcUpdate];
                s = db->update([i](auto &tx) {
                    return writer_task(tx, i);
                });
                if (!s.is_ok()) {
                    EXPECT_TRUE(is_injected_fault(s));
                    return s;
                }
                validate(*db);

                ++src_counters[kSrcView];
                s = db->view([i](const auto &tx) {
                    return reader_task(tx, i);
                });
                if (!s.is_ok()) {
                    EXPECT_TRUE(is_injected_fault(s));
                    return s;
                }
                validate(*db);

                ++src_counters[kSrcCheckpoint];
                s = db->checkpoint(kCheckpointRestart, nullptr);
                if (!s.is_ok()) {
                    EXPECT_TRUE(is_injected_fault(s));
                    return s;
                }
                CALICODB_EXPECT_TRUE(db);
                validate(*db);

                return s;
            });
        }

        TEST_LOG << " Location       | Hits per iteration\n";
        TEST_LOG << "----------------|--------------------\n";
        TEST_LOG << " kOpenDB        | " << std::setw(18) << static_cast<double>(src_counters[kSrcOpen]) / kNumIterations << '\n';
        TEST_LOG << " kUpdateDB      | " << std::setw(18) << static_cast<double>(src_counters[kSrcUpdate]) / kNumIterations << '\n';
        TEST_LOG << " kViewDB        | " << std::setw(18) << static_cast<double>(src_counters[kSrcView]) / kNumIterations << '\n';
        TEST_LOG << " kSrcCheckpoint | " << std::setw(18) << static_cast<double>(src_counters[kSrcCheckpoint]) / kNumIterations << '\n';
        TEST_LOG << '\n';
    }

    struct OpenCloseParameters {
        FaultType fault_type = kNoFaults;
        size_t num_iterations = 1;
    };
    void run_open_close_test(const OpenCloseParameters &param)
    {
        TEST_LOG << "CrashTests.OpenClose_*\n";

        Options options;
        options.env = m_env;
        options.create_if_missing = true;

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

        TEST_LOG << "Tries per iteration: " << static_cast<double>(tries) / static_cast<double>(param.num_iterations) << '\n';
    }

    struct CursorReadParameters {
        FaultType fault_type = kNoFaults;
    };
    void run_cursor_read_test(const CursorReadParameters &param)
    {
        TEST_LOG << "CrashTests.CursorRead_*\n";

        Options options;
        options.env = m_env;
        options.create_if_missing = true;
        options.sync_mode = Options::kSyncFull;

        for (size_t i = 0; i < kNumIterations; ++i) {
            hard_reset();

            DB *db;
            ASSERT_OK(ModelDB::open(options, m_filename.c_str(), m_store, db));
            ASSERT_OK(db->update([](auto &tx) {
                BucketPtr b;
                auto s = test_create_bucket_if_missing(tx, "b", b);
                if (!s.is_ok()) {
                    return s;
                }
                auto c = test_new_cursor(*b);
                auto keep_open = test_new_cursor(*b);
                if (!c || !keep_open) {
                    return Status::no_memory();
                }
                keep_open->seek_first();
                for (size_t j = 0; s.is_ok() && j < kNumRecords; ++j) {
                    const auto v = make_value(j);
                    s = b->put(make_key(j), v);
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
                return db->view([](const auto &tx) {
                    BucketPtr b;
                    auto s = test_open_bucket(tx, "b", b);
                    if (!s.is_ok()) {
                        return s;
                    }
                    auto c = test_new_cursor(*b);
                    if (!c) {
                        return Status::no_memory();
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

    void run_cursor_modify_test(const OperationsParameters &param)
    {
        TEST_LOG << "CrashTests.CursorModify_*\n";

        Options options;
        options.env = m_env;
        options.create_if_missing = true;
        options.sync_mode = param.test_sync_mode
                                ? Options::kSyncFull
                                : Options::kSyncNormal;

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
                return db->update([](auto &tx) {
                    BucketPtr b;
                    auto s = test_create_bucket_if_missing(tx, "b", b);
                    for (size_t j = 0; s.is_ok() && j < kNumRecords; ++j) {
                        const auto key = make_key(j);
                        const auto value = make_value(j);
                        s = b->put(key, value);
                    }
                    if (!s.is_ok()) {
                        return s;
                    }
                    auto c = test_new_cursor(*b);
                    if (!c) {
                        return Status::no_memory();
                    }
                    c->seek_last();
                    while (s.is_ok() && c->is_valid()) {
                        auto v = c->value().to_string();
                        v.append(v);
                        s = b->put(*c, v);
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
                        s = b->erase(*c);
                    }
                    return s;
                });
            });
            validate(*db);
            delete db;
        }
    }

    auto run_destruction_test(FaultType fault_type)
    {
        TEST_LOG << "CrashTests.Destruction_*\n";

        Options options;
        options.env = m_env;
        options.create_if_missing = true;

        run_until_completion([this, &options, fault_type] {
            const auto oom_state = m_env->m_oom_state;
            const auto syscall_state = m_env->m_syscall_state;
            hard_reset();

            DB *db;
            EXPECT_OK(ModelDB::open(options, m_filename.c_str(), m_store, db));
            EXPECT_OK(db->update([](auto &tx) {
                auto &b = tx.main_bucket();
                EXPECT_OK(b.create_bucket("a", nullptr));
                EXPECT_OK(b.create_bucket("b", nullptr));
                EXPECT_OK(b.create_bucket("c", nullptr));
                return Status::ok();
            }));
            set_fault_injection_type(fault_type);
            delete db;

            m_env->m_oom_state = oom_state;
            m_env->m_syscall_state = syscall_state;
            set_fault_injection_type(fault_type);

            return DB::destroy(options, m_filename.c_str());
        });
    }

    auto run_file_io_oom_test()
    {
        set_fault_injection_type(kOOMFaults);

        run_until_completion([this] {
            File *file = nullptr;
            auto s = m_env->new_file(m_filename.c_str(), Env::kCreate | Env::kReadWrite, file);
            if (s.is_ok()) {
                s = file->write(0, "Hello, world!");
            }
            if (s.is_ok()) {
                s = file->sync();
            }
            if (s.is_ok()) {
                char buffer[13 + 1] = {};
                s = file->read_exact(0, 13, buffer);
                if (s.is_ok()) {
                    EXPECT_EQ(Slice(buffer), "Hello, world!");
                }
            }
            delete file;
            return s;
        });
    }

    auto run_logger_oom_test()
    {
        set_fault_injection_type(kOOMFaults);

        run_until_completion([this] {
            Logger *logger;
            const auto logger_name = m_filename + "-log";
            auto s = m_env->new_logger(logger_name.c_str(), logger);
            if (s.is_ok()) {
                log(logger, "%s", std::string(1, 'a').c_str());
                log(logger, "%s", std::string(10, 'b').c_str());
                log(logger, "%s", std::string(100, 'c').c_str());
                log(logger, "%s", std::string(1'000, 'd').c_str());
                delete logger;
            }
            return s;
        });
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

TEST_F(CrashTests, InMemory_OOM)
{
    run_operations_test({kOOMFaults, false, false, true});
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

TEST_F(CrashTests, Destruction_Syscall)
{
    run_destruction_test(kSyscallFaults);
}

TEST_F(CrashTests, Destruction_OOM)
{
    run_destruction_test(kOOMFaults);
}

TEST_F(CrashTests, FileIO_OOM)
{
    run_file_io_oom_test();
}

TEST_F(CrashTests, Logger_OOM)
{
    run_logger_oom_test();
}

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

        void initialize_buffer(uint64_t file_size)
        {
            m_env->m_buffer.resize(file_size);
            EXPECT_OK(FileWrapper::read(0, file_size, m_env->m_buffer.data(), nullptr));
        }

        void perform_writes()
        {
            std::uniform_int_distribution dist;
            const auto buffer = m_env->m_buffer;
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
                ASSERT_OK(FileWrapper::write(ofs, Slice(buffer).range(ofs, len)));
            }
            // Refresh the buffer.
            EXPECT_OK(FileWrapper::read(0, m_env->m_buffer.size(), m_env->m_buffer.data(), nullptr));
        }

        auto write(uint64_t offset, const Slice &data) -> Status override
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

        auto read(uint64_t offset, size_t length, char *scratch, Slice *slice_out) -> Status override
        {
            if (m_filename != m_env->m_drop_file) {
                return FileWrapper::read(offset, length, scratch, slice_out);
            }
            uint64_t len = 0;
            if (offset < m_env->m_buffer.size()) {
                len = minval<uint64_t>(length, m_env->m_buffer.size() - offset);
                std::memcpy(scratch, m_env->m_buffer.data() + offset, len);
            }
            if (slice_out) {
                *slice_out = Slice(scratch, len);
            }
            return Status::ok();
        }

        auto resize(uint64_t size) -> Status override
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
                uint64_t file_size;
                EXPECT_OK(file->get_size(file_size));
                file->initialize_buffer(file_size);
            }
        }
        return s;
    }
};

TEST(DataLossEnvTests, NormalOperations)
{
    const auto filename = get_full_filename(testing::TempDir() + "calicodb_data_loss_env_tests");
    DataLossEnv env(default_env());
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
    const auto filename = get_full_filename(testing::TempDir() + "calicodb_data_loss_env_tests");
    DataLossEnv env(default_env());
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
        : m_filename(get_full_filename(testing::TempDir() + "calicodb_dropped_writes"))
    {
    }

    ~DataLossTests() override
    {
        delete m_db;
        delete m_env;
    }

    void reopen_db(bool clear = false)
    {
        delete m_db;
        m_db = nullptr;
        if (clear) {
            remove_calicodb_files(m_filename);
        }
        delete m_env;
        m_env = new DataLossEnv(default_env());

        Options options;
        options.env = m_env;
        options.auto_checkpoint = 0;
        options.create_if_missing = true;
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
        return m_db->update([num_writes, version, &param, this](auto &tx) {
            BucketPtr b;
            EXPECT_OK(test_create_bucket_if_missing(tx, "bucket", b));
            for (size_t i = 0; i < num_writes; ++i) {
                const auto key = numeric_key(i);
                const auto value = numeric_key(i + version * num_writes);
                EXPECT_OK(b->put(key, value));
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
        auto s = m_db->checkpoint(reset ? kCheckpointRestart : kCheckpointPassive, nullptr);
        if (!s.is_ok()) {
            EXPECT_EQ(injected_fault(), s);
            EXPECT_EQ(injected_fault(), m_db->checkpoint(reset ? kCheckpointRestart : kCheckpointPassive, nullptr));
        }
        return s;
    }

    auto check_records(size_t num_writes, size_t version)
    {
        return m_db->view([=](const auto &tx) {
            BucketPtr b;
            auto s = test_open_bucket(tx, "bucket", b);
            if (!s.is_ok()) {
                return s;
            }
            auto c = test_new_cursor(*b);
            EXPECT_NE(c, nullptr);
            for (size_t i = 0; i < num_writes && s.is_ok(); ++i) {
                const auto key = numeric_key(i);
                c->find(key);
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
                TEST_LOG << loss_type_to_string(loss_type)
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

static struct SyscallState {
    int num;
    int max;
} s_syscall_state = {};

static auto should_next_syscall_fail() -> bool
{
    if (s_syscall_state.num++ >= s_syscall_state.max) {
        s_syscall_state.num = 0;
        ++s_syscall_state.max;
        errno = EIO;
        return true;
    }
    return false;
}

static auto faulty_fcntl(int a, int b, ...) -> int
{
    int rc = 0;
    std::va_list args;
    va_start(args, b);
    if (should_next_syscall_fail()) {
        if (b == F_SETLK && F_UNLCK == va_arg(args, struct flock *)->l_type) {
            return 0; // F_UNLCK isn't checked
        }
        rc = -1;
    } else if (b == F_GETLK) {
        rc = ::fcntl(a, b, va_arg(args, struct flock *));
    }
    va_end(args);
    return rc;
}

static SyscallConfig kFaultySyscalls[] = {
    {"open", reinterpret_cast<void *>(+[](const char *a, int b, int c) -> int {
         return should_next_syscall_fail() ? -1 : ::open(a, b, c);
     })},
    {"close", reinterpret_cast<void *>(+[](int a) -> int {
         return should_next_syscall_fail() ? -1 : ::close(a);
     })},
    {"access", reinterpret_cast<void *>(+[](const char *a, int b) -> int {
         return should_next_syscall_fail() ? -1 : ::access(a, b);
     })},
    {"fstat", reinterpret_cast<void *>(+[](int a, struct stat *b) -> int {
         return should_next_syscall_fail() ? -1 : ::fstat(a, b);
     })},
    {"ftruncate", reinterpret_cast<void *>(+[](int a, off_t b) -> int {
         return should_next_syscall_fail() ? -1 : ::ftruncate(a, b);
     })},
    {"fcntl", reinterpret_cast<void *>(faulty_fcntl)},
    {"lseek", reinterpret_cast<void *>(+[](int a, off_t b, int c) -> off_t {
         return should_next_syscall_fail() ? -1 : ::lseek(a, b, c);
     })},
    {"read", reinterpret_cast<void *>(+[](int a, void *b, size_t c) -> ssize_t {
         return should_next_syscall_fail() ? -1 : ::read(a, b, c);
     })},
    {"write", reinterpret_cast<void *>(+[](int a, const void *b, size_t c) -> ssize_t {
         return should_next_syscall_fail() ? -1 : ::write(a, b, c);
     })},
    {"fsync", reinterpret_cast<void *>(+[](int a) -> int {
         return should_next_syscall_fail() ? -1 : ::fsync(a);
     })},
    {"unlink", reinterpret_cast<void *>(+[](const char *a) -> int {
         return should_next_syscall_fail() ? -1 : ::unlink(a);
     })},
    {"mmap", reinterpret_cast<void *>(+[](void *a, size_t b, int c, int d, int e, off_t f) -> void * {
         return should_next_syscall_fail() ? MAP_FAILED : ::mmap(a, b, c, d, e, f);
     })},
    {"munmap", reinterpret_cast<void *>(+[](void *a, size_t b) -> int {
         return should_next_syscall_fail() ? -1 : ::munmap(a, b);
     })},
    {"readlink", reinterpret_cast<void *>(+[](const char *a, char *b, size_t c) -> ssize_t {
         return should_next_syscall_fail() ? -1 : ::readlink(a, b, c);
     })},
    {"lstat", reinterpret_cast<void *>(+[](const char *a, struct stat *b) -> int {
         return should_next_syscall_fail() ? -1 : ::lstat(a, b);
     })},
    {"getcwd", reinterpret_cast<void *>(+[](char *a, size_t b) -> char * {
         return should_next_syscall_fail() ? nullptr : ::getcwd(a, b);
     })},
    {"stat", reinterpret_cast<void *>(+[](const char *a, struct stat *b) -> int {
         return should_next_syscall_fail() ? -1 : ::stat(a, b);
     })},
};

static const size_t kNumSyscalls = ARRAY_SIZE(kFaultySyscalls);

class SyscallCrashTests : public testing::TestWithParam<uint32_t>
{
public:
    const std::string m_filename;

    SyscallCrashTests()
        : m_filename(testing::TempDir() + "calicodb_syscall_crash_tests")
    {
        remove_calicodb_files(m_filename);
    }

    ~SyscallCrashTests() override = default;

    template <class Setup, class Run>
    void run_test(Setup &&setup, Run &&run)
    {
        setup();
        ASSERT_OK(run()); // Sanity check: no faults

        for (size_t i = 0; i < kNumSyscalls; ++i) {
            s_syscall_state = {};

            Status s;
            do {
                setup();
                ASSERT_OK(configure(kReplaceSyscall, &kFaultySyscalls[i]));
                s = run();
                ASSERT_OK(configure(kRestoreSyscall, kFaultySyscalls[i].name));
            } while (!s.is_ok());
        }
    }

    static auto write_to_db(DB &db)
    {
        return db.update([](auto &tx) {
            Status s;
            RandomGenerator random;
            auto &b = tx.main_bucket();
            for (size_t i = 0; s.is_ok() && i < 500; ++i) {
                s = b.put(numeric_key(i), random.Generate(250));
            }
            return s;
        });
    }
};

TEST_F(SyscallCrashTests, UnrecognizedSyscall)
{
    SyscallConfig config = {"open", nullptr};
    ASSERT_NOK(configure(kReplaceSyscall, &config));
    config.name = "not_a_syscall";
    config.syscall = &kFaultySyscalls[0];
    ASSERT_NOK(configure(kReplaceSyscall, &config));
    ASSERT_NOK(configure(kRestoreSyscall, config.name));
}

TEST_F(SyscallCrashTests, OpenClose)
{
    run_test([] {}, [this] {
        DBPtr db;
        Options options;
        options.create_if_missing = true;
        remove_calicodb_files(m_filename);
        return test_open_db(options, m_filename, db); });
}

TEST_F(SyscallCrashTests, Writes)
{
    DBPtr db;
    run_test(
        [&db, this] {
            Options options;
            options.create_if_missing = true;
            options.sync_mode = Options::kSyncFull;
            remove_calicodb_files(m_filename);
            ASSERT_OK(test_open_db(options, m_filename, db));
        },
        [&db] {
            auto s = write_to_db(*db);
            if (s.is_ok()) {
                s = db->checkpoint(kCheckpointRestart, nullptr);
            }
            return s;
        });
}

TEST_F(SyscallCrashTests, Reads)
{
    DBPtr db;
    run_test(
        [&db, this] {
            Options options;
            options.create_if_missing = true;
            remove_calicodb_files(m_filename);
            ASSERT_OK(test_open_db(options, m_filename, db));
            ASSERT_OK(write_to_db(*db));
        },
        [&db] {
            return db->view([](auto &tx) {
                auto c = test_new_cursor(tx.main_bucket());
                c->seek_first();
                for (size_t i = 0; c->is_valid(); ++i) {
                    EXPECT_EQ(c->key(), numeric_key(i));
                    c->next();
                }
                return c->status();
            });
        });
}

TEST_F(SyscallCrashTests, InterruptedOpen)
{
    const auto faulty_open = [](const char *, int, int) -> int {
        errno = EINTR;
        return -1;
    };
    const SyscallConfig config = {"open", reinterpret_cast<void *>(+faulty_open)};
    ASSERT_OK(configure(kReplaceSyscall, &config));

    File *file;
    Logger *logger;
    auto &env = default_env();
    remove_calicodb_files(m_filename);
    ASSERT_NOK(env.new_file(m_filename.c_str(), Env::kCreate, file));
    ASSERT_NOK(env.new_logger(m_filename.c_str(), logger));

    ASSERT_OK(configure(kRestoreSyscall, "open"));
}

TEST_F(SyscallCrashTests, InterruptedClose)
{
    const auto faulty_close = [](int) -> int {
        errno = EINTR;
        return -1;
    };
    const SyscallConfig config = {"close", reinterpret_cast<void *>(+faulty_close)};
    ASSERT_OK(configure(kReplaceSyscall, &config));

    File *file;
    auto &env = default_env();
    remove_calicodb_files(m_filename);
    ASSERT_OK(env.new_file(m_filename.c_str(), Env::kCreate, file));
    delete file;

    ASSERT_OK(configure(kRestoreSyscall, "close"));
}

TEST_F(SyscallCrashTests, InterruptedFtruncate)
{
    File *file;
    auto &env = default_env();
    remove_calicodb_files(m_filename);
    ASSERT_OK(env.new_file(m_filename.c_str(), Env::kCreate, file));

    const auto faulty_ftruncate = [](int, off_t) -> int {
        errno = EINTR;
        return -1;
    };
    const SyscallConfig config = {"ftruncate", reinterpret_cast<void *>(+faulty_ftruncate)};
    ASSERT_OK(configure(kReplaceSyscall, &config));
    ASSERT_NOK(file->resize(123));
    ASSERT_OK(configure(kRestoreSyscall, "ftruncate"));
    delete file;
}

static auto faulty_lock(int, ...) -> int
{
    errno = EACCES;
    return -1;
}

TEST_F(SyscallCrashTests, FileLock)
{
    File *file;
    auto &env = default_env();
    remove_calicodb_files(m_filename);
    ASSERT_OK(env.new_file(m_filename.c_str(), Env::kCreate, file));

    const SyscallConfig config = {"fcntl", reinterpret_cast<void *>(faulty_lock)};
    ASSERT_OK(configure(kReplaceSyscall, &config));
    const auto s = file->file_lock(kFileShared);
    ASSERT_TRUE(s.is_busy()) << s.message();
    ASSERT_OK(configure(kRestoreSyscall, "fcntl"));
    delete file;
}

} // namespace calicodb::test