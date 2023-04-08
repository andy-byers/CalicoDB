// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_TEST_UNIT_TESTS_H
#define CALICODB_TEST_UNIT_TESTS_H

#include "calicodb/status.h"
#include "db_impl.h"
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

[[nodiscard]] static auto db_impl(const DB *db) -> const DBImpl *
{
    return reinterpret_cast<const DBImpl *>(db);
}

[[nodiscard]] static auto db_impl(DB *db) -> DBImpl *
{
    return reinterpret_cast<DBImpl *>(db);
}

[[nodiscard]] static auto table_impl(const Table *table) -> const TableImpl *
{
    return reinterpret_cast<const TableImpl *>(table);
}

[[nodiscard]] static auto table_impl(Table *table) -> TableImpl *
{
    return reinterpret_cast<TableImpl *>(table);
}

#define CLEAR_INTERCEPTORS()                                                 \
    do {                                                                     \
        dynamic_cast<tools::FaultInjectionEnv &>(*env).clear_interceptors(); \
    } while (0)

#define QUICK_INTERCEPTOR(prefix__, type__)                                  \
    do {                                                                     \
        dynamic_cast<tools::FaultInjectionEnv &>(*env)                       \
            .add_interceptor(tools::Interceptor {(prefix__), (type__), [] {  \
                                                     return special_error(); \
                                                 }});                        \
    } while (0)

#define COUNTING_INTERCEPTOR(prefix__, type__, n__)                                   \
    do {                                                                              \
        dynamic_cast<tools::FaultInjectionEnv &>(*env)                                \
            .add_interceptor(tools::Interceptor {(prefix__), (type__), [&n = (n__)] { \
                                                     if (n-- <= 0) {                  \
                                                         return special_error();      \
                                                     }                                \
                                                     return Status::ok();             \
                                                 }});                                 \
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

class InMemoryTest
{
public:
    const std::string kFilename {"./test"};

    InMemoryTest()
        : env {std::make_unique<tools::FakeEnv>()}
    {
    }

    virtual ~InMemoryTest() = default;

    [[nodiscard]] auto get_env() -> tools::FakeEnv &
    {
        return dynamic_cast<tools::FakeEnv &>(*env);
    }

    std::unique_ptr<Env> env;
};

class OnDiskTest
{
public:
    const std::string kTestDir {"./test_dir"};
    const std::string kFilename {join_paths(kTestDir, "test")};

    OnDiskTest()
        : env {Env::default_env()}
    {
        std::filesystem::remove_all(kTestDir);
        std::filesystem::create_directory(kTestDir);
    }

    virtual ~OnDiskTest()
    {
        std::filesystem::remove_all(kTestDir);
    }

    std::unique_ptr<Env> env;
};

class TestWithPager : public InMemoryTest
{
public:
    static constexpr auto kPageSize = kMinPageSize;
    static constexpr auto kFrameCount = kMinFrameCount;

    TestWithPager()
        : scratch(kPageSize, '\x00'),
          random(1'024 * 1'024 * 8)
    {
        wal = std::make_unique<tools::FakeWal>(Wal::Parameters {
            kFilename,
            kPageSize,
            env.get(),
        });
        tables.add(LogicalPageId::with_table(Id::root()));
        Pager *temp;
        EXPECT_OK(Pager::open({
                                  kFilename,
                                  env.get(),
                                  wal.get(),
                                  nullptr,
                                  &state,
                                  kFrameCount,
                                  kPageSize,
                              },
                              temp));
        pager.reset(temp);
        state.use_wal = false;
    }

    DBState state;
    TableSet tables;
    std::string scratch;
    std::string collect_scratch;
    std::unique_ptr<Pager> pager;
    std::unique_ptr<tools::FakeWal> wal;
    tools::RandomGenerator random;
};

inline auto expect_ok(const Status &s) -> void
{
    if (!s.is_ok()) {
        std::fprintf(stderr, "unexpected %s status: %s\n", get_status_name(s), s.to_string().data());
        std::abort();
    }
}

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

namespace test_tools
{

template <class T>
auto get(T &t, const std::string &key, std::string *value) -> Status
{
    return t.get(key, value);
}

template <class T>
auto find(T &t, const std::string &key) -> Cursor *
{
    auto *cursor = t.new_cursor();
    if (cursor) {
        cursor->seek(key);
    }
    return cursor;
}

template <class T>
auto contains(T &t, const std::string &key) -> bool
{
    std::string value;
    return get(t, key, value).is_ok();
}

template <class T>
auto contains(T &t, const std::string &key, const std::string &value) -> bool
{
    std::string val;
    if (auto s = get(t, key, val); s.is_ok()) {
        return val == value;
    }
    return false;
}

template <class T>
auto expect_contains(T &t, const std::string &key, const std::string &value) -> void
{
    std::string val;
    if (auto s = get(t, key, &val); s.is_ok()) {
        if (val != value) {
            std::cerr << "value does not match (\"" << value << "\" != \"" << val << "\")\n";
            std::abort();
        }
    } else {
        std::cerr << "could not find key " << key << '\n';
        std::abort();
    }
}

template <class T>
auto insert(T &t, const std::string &key, const std::string &value) -> void
{
    auto s = t.add(key, value);
    if (!s.is_ok()) {
        std::fputs(s.to_string().data(), stderr);
        std::abort();
    }
}

template <class T>
auto erase(T &t, const std::string &key) -> bool
{
    auto s = t.erase(get(t, key));
    if (!s.is_ok() && !s.is_not_found()) {
        std::fputs(s.to_string().data(), stderr);
        std::abort();
    }
    return !s.is_not_found();
}

template <class T>
auto erase_one(T &t, const std::string &key) -> bool
{
    auto was_erased = t.erase(get(t, key));
    EXPECT_TRUE(was_erased.has_value());
    if (was_erased.value())
        return true;
    auto cursor = t.first();
    EXPECT_EQ(cursor.error(), std::nullopt);
    if (!cursor.is_valid())
        return false;
    was_erased = t.erase(cursor);
    EXPECT_TRUE(was_erased.value());
    return true;
}

inline auto write_file(Env &env, const std::string &path, Slice in) -> void
{
    File *file;
    ASSERT_OK(env.new_file(path, file));
    ASSERT_OK(file->write(0, in));
    delete file;
}

inline auto append_file(Env &env, const std::string &path, Slice in) -> void
{
    std::size_t file_size;
    ASSERT_OK(env.file_size(path, file_size));

    File *file;
    ASSERT_OK(env.new_file(path, file));
    ASSERT_OK(file->write(file_size, in));
    delete file;
}

inline auto read_file(Env &env, const std::string &path) -> std::string
{
    File *file;
    std::string out;
    std::size_t size;

    EXPECT_TRUE(env.file_size(path, size).is_ok());
    EXPECT_TRUE(env.new_file(path, file).is_ok());
    out.resize(size);

    EXPECT_TRUE(file->read_exact(0, size, out.data()).is_ok());
    delete file;
    return out;
}
} // namespace test_tools

struct Record {
    inline auto operator<(const Record &rhs) const -> bool
    {
        return Slice(key) < Slice(rhs.key);
    }

    std::string key;
    std::string value;
};

auto operator>(const Record &, const Record &) -> bool;
auto operator<=(const Record &, const Record &) -> bool;
auto operator>=(const Record &, const Record &) -> bool;
auto operator==(const Record &, const Record &) -> bool;
auto operator!=(const Record &, const Record &) -> bool;

class RecordGenerator
{
public:
    static unsigned default_seed;

    struct Parameters {
        std::size_t mean_key_size = 12;
        std::size_t mean_value_size = 18;
        std::size_t spread = 4;
        bool is_sequential = false;
        bool is_unique = false;
    };

    RecordGenerator() = default;
    explicit RecordGenerator(Parameters);
    auto generate(tools::RandomGenerator &, std::size_t) const -> std::vector<Record>;

private:
    Parameters m_param;
};

} // namespace calicodb

#endif // CALICODB_TEST_UNIT_TESTS_H
