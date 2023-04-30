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
            m_env = Env::default_env();
        } else {
            m_env = new EnvType();
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
        delete m_env;
    }

    [[nodiscard]] auto env() -> EnvType &
    {
        return reinterpret_cast<EnvType &>(*m_env);
    }

    [[nodiscard]] auto env() const -> const EnvType &
    {
        return reinterpret_cast<const EnvType &>(*m_env);
    }

protected:
    Env *m_env;
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
        // Spawn "param.num_processes-1" processes (1 test runs in this process to help
        // with debugging).
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
                        break;
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
        for (std::size_t n = 1; n < param.num_processes; ++n) {
            int s;
            const auto pid = wait(&s);
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

static const auto kConcurrencySanityCheckValues = ::testing::Values(
    ConcurrencyTestParam{1, 1});

static const auto kMultiThreadConcurrencyValues = ::testing::Values(
    ConcurrencyTestParam{1, 2},
    ConcurrencyTestParam{1, 4},
    ConcurrencyTestParam{1, 6});

static const auto kMultiProcessConcurrencyValues = ::testing::Values(
    ConcurrencyTestParam{2, 1},
    ConcurrencyTestParam{4, 1},
    ConcurrencyTestParam{6, 1});

static const auto kMultiProcessMultiThreadConcurrencyValues = ::testing::Values(
    ConcurrencyTestParam{2, 2},
    ConcurrencyTestParam{3, 3},
    ConcurrencyTestParam{4, 4});

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
