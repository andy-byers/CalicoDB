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
#include "wal_writer.h"
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

#define EXPECT_OK(expr)                                                                                                     \
    do {                                                                                                                    \
        const auto &expect_ok_status = (expr);                                                                              \
        EXPECT_TRUE(expect_ok_status.is_ok()) << get_status_name(expect_ok_status) << ": " << expect_ok_status.to_string(); \
    } while (0)

#define ASSERT_OK(expr)                                                                                                     \
    do {                                                                                                                    \
        const auto &expect_ok_status = (expr);                                                                              \
        ASSERT_TRUE(expect_ok_status.is_ok()) << get_status_name(expect_ok_status) << ": " << expect_ok_status.to_string(); \
    } while (0)

[[nodiscard]] inline auto expose_message(const Status &s)
{
    EXPECT_TRUE(s.is_ok()) << "Unexpected " << get_status_name(s) << " status: " << s.to_string().data();
    return s.is_ok();
}

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

class DisabledWriteAheadLog : public WriteAheadLog
{
public:
    DisabledWriteAheadLog() = default;
    ~DisabledWriteAheadLog() override = default;

    [[nodiscard]] auto flushed_lsn() const -> Id override
    {
        return {std::numeric_limits<std::size_t>::max()};
    }

    [[nodiscard]] auto current_lsn() const -> Id override
    {
        return Id::null();
    }

    [[nodiscard]] auto bytes_written() const -> std::size_t override
    {
        return 0;
    }

    [[nodiscard]] auto log_delta(Id, const Slice &, const ChangeBuffer &, Lsn *) -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]] auto log_image(Id, const Slice &, Lsn *) -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]] auto log_vacuum(bool, Lsn *) -> Status override
    {
        return Status::ok();
    }

    auto flush() -> Status override
    {
        return Status::ok();
    }

    auto cleanup(Id) -> Status override
    {
        return Status::ok();
    }
};

class TestWithPager : public InMemoryTest
{
public:
    const std::size_t kPageSize {0x200};
    const std::size_t kFrameCount {16};

    TestWithPager()
        : scratch(kPageSize, '\x00')
    {
        tables.add(LogicalPageId::with_table(Id::root()));
        Pager *temp;
        EXPECT_OK(Pager::open({
                                  kFilename,
                                  env.get(),
                                  &wal,
                                  nullptr,
                                  &state,
                                  kFrameCount,
                                  kPageSize,
                              },
                              &temp));
        pager.reset(temp);
    }

    DBState state;
    TableSet tables;
    DisabledWriteAheadLog wal;
    std::string scratch;
    std::string collect_scratch;
    std::unique_ptr<Pager> pager;
    tools::RandomGenerator random {1'024 * 1'024 * 8};
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
    return Status::system_error("42");
}

inline auto assert_special_error(const Status &s)
{
    if (!s.is_system_error() || s.to_string() != special_error().to_string()) {
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
    Editor *file;
    ASSERT_TRUE(env.new_editor(path, file).is_ok());
    ASSERT_TRUE(file->write(0, in).is_ok());
    delete file;
}

inline auto append_file(Env &env, const std::string &path, Slice in) -> void
{
    Logger *file;
    ASSERT_TRUE(env.new_logger(path, file).is_ok());
    ASSERT_TRUE(file->write(in).is_ok());
    delete file;
}

inline auto read_file(Env &env, const std::string &path) -> std::string
{
    Reader *file;
    std::string out;
    std::size_t size;

    EXPECT_TRUE(env.file_size(path, size).is_ok());
    EXPECT_TRUE(env.new_reader(path, file).is_ok());
    out.resize(size);

    Slice slice;
    EXPECT_TRUE(file->read(0, size, out.data(), &slice).is_ok());
    EXPECT_EQ(slice.size(), size);
    delete file;
    return out;
}
} // namespace test_tools

struct Record {
    inline auto operator<(const Record &rhs) const -> bool
    {
        return Slice {key} < Slice {rhs.key};
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
        std::size_t mean_key_size {12};
        std::size_t mean_value_size {18};
        std::size_t spread {4};
        bool is_sequential {};
        bool is_unique {};
    };

    RecordGenerator() = default;
    explicit RecordGenerator(Parameters);
    auto generate(tools::RandomGenerator &, std::size_t) const -> std::vector<Record>;

private:
    Parameters m_param;
};

} // namespace calicodb

#endif // CALICODB_TEST_UNIT_TESTS_H
