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

//[[nodiscard]] static auto db_impl(const DB *db) -> const DBImpl *
//{
//    return reinterpret_cast<const DBImpl *>(db);
//}
//
//[[nodiscard]] static auto db_impl(DB *db) -> DBImpl *
//{
//    return reinterpret_cast<DBImpl *>(db);
//}

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
