// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "common.h"
#include "env_helpers.h"
#include "scope_guard.h"
#include "tools.h"
#include <gtest/gtest.h>
#include <thread>

namespace calicodb::test::crashes
{

struct TestState {
    std::size_t tries = 0;
    Env *env = nullptr;
};
using TestRoutine = std::function<Status(TestState)>;
class TestHarness
{
public:
    explicit TestHarness(Env &env)
        : m_env(new tools::TestEnv(env)),
          m_mxcounts(kMaxCounters),
          m_counters(kMaxCounters)
    {
    }

    virtual ~TestHarness()
    {
        delete m_env;
    }

    auto register_fault(const std::string &filename, tools::SyscallType syscall) -> void
    {
        const auto index = m_num_counters++;
        CALICODB_EXPECT_LT(index, kMaxCounters);
        m_env->add_interceptor(
            filename,
            tools::Interceptor(syscall, [index, this] {
                if (m_counters[index]++ >= m_mxcounts[index]) {
                    m_counters[index] = 0;
                    ++m_mxcounts[index];
                    return Status::io_error("FAULT");
                }
                return Status::ok();
            }));
    }
    auto clear_faults() -> void
    {
        std::cerr
            << "[FAULTS: "
            << std::accumulate(begin(m_mxcounts), end(m_mxcounts), 0)
            << "]\n";

        m_env->clear_interceptors();
        m_counters = m_mxcounts = {};
        m_num_counters = 0;
    }

    [[nodiscard]] auto test(const TestRoutine &routine) -> Status
    {
        Status s;
        // Run the test routine repeatedly until it passes. Each time a fault is hit, allow 1 additional
        // success before returning with another fault.
        for (TestState st{0, m_env}; !(s = routine(st)).is_ok(); ++st.tries) {
            if (!s.is_io_error() || s.to_string() != "FAULT") {
                break;
            }
        }
        return s;
    }

protected:
    tools::TestEnv *m_env;

    static constexpr std::size_t kMaxCounters = 16;
    std::vector<int> m_mxcounts;
    std::vector<int> m_counters;
    std::size_t m_num_counters = 0;
};

static constexpr const char *kDBName = "./crashDB";
static constexpr const char *kWalName = "./crashDB-wal";
static constexpr const char *kShmName = "./crashDB-shm";

using CrashTestParam = std::tuple<
    std::string,
    tools::SyscallType>;
class CrashTests
    : public testing::TestWithParam<CrashTestParam>,
      public TestHarness
{
protected:
    static constexpr std::size_t kNumTables = 4;
    static constexpr std::size_t kNumRecords = 1'000;

    explicit CrashTests()
        : TestHarness(*Env::default_env())
    {
        std::filesystem::remove_all(kDBName);
        std::filesystem::remove_all(kWalName);
        std::filesystem::remove_all(kShmName);
    }

    ~CrashTests() override
    {
        delete m_txn;
        delete m_db;
    }

    auto SetUp() -> void override
    {
        ASSERT_OK(reopen(true));

        // Add a batch of records, then checkpoint.
        ASSERT_OK(m_routine.run(*m_txn));
        ++m_routine.round;
        ASSERT_OK(m_txn->commit());
        delete m_txn;
        m_txn = nullptr;
        ASSERT_OK(m_db->checkpoint(true));
        ASSERT_OK(reopen(true));

        register_fault(
            std::get<0>(GetParam()),
            std::get<1>(GetParam()));
    }

    [[nodiscard]] auto reopen(bool write) -> Status
    {
        delete m_txn;
        delete m_db;
        m_txn = nullptr;
        m_db = nullptr;

        Options dbopt;
        dbopt.env = m_env;
        CALICODB_TRY(DB::open(dbopt, kDBName, m_db));
        return m_db->new_txn(write, m_txn);
    }

    auto end_txn_and_validate() -> void
    {
        clear_faults();

        // Transaction must be finished when a checkpoint is run.
        delete m_txn;
        m_txn = nullptr;
        ASSERT_OK(m_db->checkpoint(false));
        ASSERT_OK(reopen(false));
        ASSERT_OK(m_routine.check(*m_txn, true));
    }

    std::map<std::string, std::string> m_constant;

    TransferBatch m_routine{kNumTables, kNumRecords};
    Txn *m_txn = nullptr;
    DB *m_db = nullptr;
};

// System call failures are followed by calls to `Txn::rollback()`, which is expected to
// fix state inconsistencies, as well as undo any modifications made since the last commit.
// Each time `run_until_crash()` runs, the database look exactly the same.
TEST_P(CrashTests, CrashRollback)
{
    ASSERT_OK(test([this](auto state) {
        auto s = m_routine.run(*m_txn);
        if (s.is_ok()) {
            s = m_txn->commit();
        }
        if (!s.is_ok()) {
            m_txn->rollback();
        }
        return s;
    }));
    end_txn_and_validate();
}

TEST_P(CrashTests, CrashRecovery)
{
    ASSERT_OK(test([this](auto state) {
        auto s = reopen(true);
        if (s.is_ok()) {
            s = m_routine.run(*m_txn);
            if (s.is_ok()) {
                s = m_txn->commit();
            }
        }
        return s;
    }));
    end_txn_and_validate();
}

static auto label_testcase(const testing::TestParamInfo<CrashTestParam> &info) -> std::string
{
    std::string label;

    const auto filename = std::get<0>(info.param);
    if (filename == kDBName) {
        label.append("DB");
    } else if (filename == kWalName) {
        label.append("WAL");
    } else if (filename == kShmName) {
        label.append("shm");
    }
    const auto mask = std::get<1>(info.param);
    if (mask & tools::kSyscallRead) {
        label.append("_Read");
    }
    if (mask & tools::kSyscallWrite) {
        label.append("_Write");
    }
    if (mask & tools::kSyscallOpen) {
        label.append("_Open");
    }
    if (mask & tools::kSyscallSync) {
        label.append("_Sync");
    }
    if (mask & tools::kSyscallResize) {
        label.append("_Resize");
    }
    return label;
}
INSTANTIATE_TEST_CASE_P(
    SingleFile,
    CrashTests,
    testing::Combine(
        testing::Values(
            kDBName,
            kWalName),
        testing::Values(
            tools::kSyscallOpen,
            tools::kSyscallRead,
            tools::kSyscallWrite,
            tools::kSyscallSync,
            tools::kSyscallResize)),
    label_testcase);

} // namespace calicodb::test::crashes
