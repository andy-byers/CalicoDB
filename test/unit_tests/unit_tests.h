// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_TEST_UNIT_TESTS_H
#define CALICODB_TEST_UNIT_TESTS_H

#include "calicodb/status.h"
#include "db_impl.h"
#include "env_helpers.h"
#include "env_posix.h"
#include "page.h"
#include "tools.h"
#include "utils.h"
#include "wal.h"
#include <atomic>
#include <filesystem>
#include <gtest/gtest.h>
#include <iomanip>
#include <sstream>

namespace calicodb
{

static constexpr auto kDBFilename = "./_test-db";
static constexpr auto kWalFilename = "./_test-wal";
static constexpr auto kShmFilename = "./_test-shm";

#define CLEAR_INTERCEPTORS()        \
    do {                            \
        env().clear_interceptors(); \
    } while (0)

#define QUICK_INTERCEPTOR(filename__, type__)                                            \
    do {                                                                                 \
        env().add_interceptor(filename__, tools::Interceptor{(type__), [] {              \
                                                                 return special_error(); \
                                                             }});                        \
    } while (0)

#define COUNTING_INTERCEPTOR(filename__, type__, n__)                                        \
    do {                                                                                     \
        env().add_interceptor(filename__, tools::Interceptor{(type__), [&n = (n__)] {        \
                                                                 if (n-- <= 0) {             \
                                                                     return special_error(); \
                                                                 }                           \
                                                                 return Status::ok();        \
                                                             }});                            \
    } while (0)

static constexpr auto kExpectationMatcher = "^expectation";

#define STREAM_MESSAGE(expr) #expr                                    \
                                 << " == Status::ok()\" but got \""   \
                                 << get_status_name(expect_ok_status) \
                                 << "\" status with message \""       \
                                 << expect_ok_status.to_string()      \
                                 << "\"\n";

#define EXPECT_OK(expr)                        \
    do {                                       \
        const auto &expect_ok_status = (expr); \
        EXPECT_TRUE(expect_ok_status.is_ok())  \
            << "expected \""                   \
            << STREAM_MESSAGE(expr);           \
    } while (0)

#define ASSERT_OK(expr)                        \
    do {                                       \
        const auto &expect_ok_status = (expr); \
        ASSERT_TRUE(expect_ok_status.is_ok())  \
            << "asserted \""                   \
            << STREAM_MESSAGE(expr);           \
    } while (0)

template <class EnvType>
class EnvTestHarness
{
public:
    explicit EnvTestHarness()
    {
        if constexpr (std::is_same_v<EnvType, PosixEnv>) {
            m_env = new tools::TestEnv(*Env::default_env());
        } else if constexpr (!std::is_same_v<EnvType, tools::TestEnv>) {
            m_env = new tools::TestEnv(*new EnvType());
        } else {
            m_env = new tools::TestEnv;
        }
        (void)m_env->remove_file(kDBFilename);
        (void)m_env->remove_file(kWalFilename);
        (void)m_env->remove_file(kShmFilename);
    }

    virtual ~EnvTestHarness()
    {
        (void)m_env->remove_file(kDBFilename);
        (void)m_env->remove_file(kWalFilename);
        (void)m_env->remove_file(kShmFilename);
        // tools::TestEnv deletes the wrapped Env.
        delete m_env;
    }

    [[nodiscard]] auto env() -> tools::TestEnv &
    {
        return *m_env;
    }

    [[nodiscard]] auto env() const -> const tools::TestEnv &
    {
        return *m_env;
    }

protected:
    tools::TestEnv *m_env;
};

template <class EnvType>
class PagerTestHarness : public EnvTestHarness<EnvType>
{
public:
    using Base = EnvTestHarness<EnvType>;
    static constexpr auto kFrameCount = kMinFrameCount;

    PagerTestHarness()
    {
        FileHeader header;
        std::string buffer(kPageSize, '\0');
        header.page_count = 1;
        header.write(buffer.data());
        tools::write_string_to_file(Base::env(), kDBFilename, buffer);

        File *file;
        EXPECT_OK(Base::env().new_file(kDBFilename, Env::kCreate, file));

        const Pager::Parameters pager_param = {
            kDBFilename,
            kWalFilename,
            file,
            &Base::env(),
            nullptr,
            &m_state,
            nullptr,
            kFrameCount,
        };

        CHECK_OK(Pager::open(pager_param, m_pager));
        m_pager->set_page_count(1);
        m_state.use_wal = true;
    }

    ~PagerTestHarness() override
    {
        (void)m_pager->close();
        delete m_pager;
        m_pager = nullptr;
    }

protected:
    DBState m_state;
    Pager *m_pager = nullptr;
};

struct ConcurrencyTestParam {
    std::size_t num_processes = 0;
    std::size_t num_threads = 0;
};
template <class EnvType>
class ConcurrencyTestHarness : public EnvTestHarness<EnvType>
{
    using Base = EnvTestHarness<EnvType>;

public:
    explicit ConcurrencyTestHarness()
    {
        register_main_callback(
            [](auto &) {
                // Main callback is optional. Defaults to falling through and waiting
                // on child processes to complete.
                return false;
            });
        register_test_callback(
            [](auto &, auto, auto) {
                ADD_FAILURE() << "test instance was not registered";
                return false;
            });
    }

    ~ConcurrencyTestHarness() override = default;

    using MainRoutine = std::function<bool(Env &)>;
    using TestInstance = std::function<bool(Env &, std::size_t, std::size_t)>;
    auto register_main_callback(MainRoutine main) -> void
    {
        m_main = std::move(main);
    }
    auto register_test_callback(TestInstance test) -> void
    {
        m_test = std::move(test);
    }

    // Run a test in multiple threads/processes
    // Each instance of the test is passed "env", an instance of the Env type that
    // this class template was instantiated with, "n" and "t", indices in the range
    // [0,param.num_processes-1] and [0,param.num_threads-1], respectively,
    // representing the process and thread running the test instance. The test
    // callback should return true if it should continue running, false otherwise.
    auto run_test(const ConcurrencyTestParam &param) -> void
    {
        ASSERT_TRUE(m_test) << "REQUIRES: register_test_callback() was called";
        // Spawn "param.num_processes" processes.
        for (std::size_t n = 0; n < param.num_processes; ++n) {
            const auto pid = fork();
            ASSERT_NE(-1, pid)
                << "fork(): " << strerror(errno);
            if (pid) {
                continue;
            }
            // For each process, spawn "param.num_threads" threads.
            std::vector<std::thread> threads;
            for (std::size_t t = 0; t < param.num_threads; ++t) {
                threads.emplace_back([n, t, this] {
                    // Run the test callback for each thread.
                    while (m_test(Base::env(), n, t)) {
                    }
                });
            }

            for (auto &thread : threads) {
                thread.join();
            }
            std::exit(testing::Test::HasFailure());
        }
        while (m_main(Base::env())) {
        }
        struct Result {
            pid_t pid;
            int s;
        };
        std::vector<Result> results;
        for (std::size_t n = 0; n < param.num_processes; ++n) {
            Result r;
            r.pid = wait(&r.s);
            results.emplace_back(r);
        }
        for (auto [pid, s] : results) {
            ASSERT_NE(pid, -1)
                << "wait(): " << strerror(errno);
            ASSERT_TRUE(WIFEXITED(s) && WEXITSTATUS(s) == 0)
                << "exited " << (WIFEXITED(s) ? "" : "ab")
                << "normally with exit status "
                << WEXITSTATUS(s);
        }
    }

private:
    MainRoutine m_main;
    TestInstance m_test;
};
inline auto label_concurrency_test(std::string base, const testing::TestParamInfo<std::tuple<std::size_t, std::size_t>> &info) -> std::string
{
    append_number(base, std::get<0>(info.param));
    base.append("P_");
    append_number(base, std::get<1>(info.param));
    return base + 'T';
}

class SharedCount
{
    volatile U32 *m_ptr = nullptr;
    File *m_file = nullptr;

public:
    explicit SharedCount(Env &env, const std::string &name)
    {
        volatile void *ptr;
        CHECK_OK(env.new_file(name, Env::kCreate | Env::kReadWrite, m_file));
        CHECK_OK(m_file->shm_map(0, true, ptr));
        CHECK_TRUE(ptr);
        m_ptr = reinterpret_cast<volatile U32 *>(ptr);
    }

    ~SharedCount()
    {
        m_file->shm_unmap(true);
        delete m_file;
    }

    enum MemoryOrder : int {
        kRelaxed = __ATOMIC_RELAXED,
        kAcquire = __ATOMIC_ACQUIRE,
        kRelease = __ATOMIC_RELEASE,
        kAcqRel = __ATOMIC_ACQ_REL,
        kSeqCst = __ATOMIC_SEQ_CST,
    };
    auto load(MemoryOrder order = kAcquire) const -> U32
    {
        return __atomic_load_n(m_ptr, order);
    }
    auto store(U32 value, MemoryOrder order = kRelease) -> void
    {
        __atomic_store_n(m_ptr, value, order);
    }
    auto increase(U32 n, MemoryOrder order = kRelaxed) -> U32
    {
        return __atomic_add_fetch(m_ptr, n, order);
    }
};

[[nodiscard]] inline auto special_error()
{
    return Status::io_error("42");
}

inline auto assert_special_error(const Status &s)
{
    if (!s.is_io_error() || s.to_string() != special_error().to_string()) {
        std::fprintf(stderr, "error: unexpected %s status: %s", get_status_name(s), s.is_ok() ? "NULL" : s.to_string().data());
        std::abort();
    }
}

} // namespace calicodb

#endif // CALICODB_TEST_UNIT_TESTS_H
