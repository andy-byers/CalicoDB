#ifndef CALICODB_TEST_UNIT_TESTS_H
#define CALICODB_TEST_UNIT_TESTS_H

#include "calicodb/status.h"
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

#define Clear_Interceptors()                                                 \
    do {                                                                     \
        dynamic_cast<Tools::DynamicMemory &>(*storage).clear_interceptors(); \
    } while (0)

#define Quick_Interceptor(prefix__, type__)                                  \
    do {                                                                     \
        dynamic_cast<Tools::DynamicMemory &>(*storage)                       \
            .add_interceptor(Tools::Interceptor {(prefix__), (type__), [] {  \
                                                     return special_error(); \
                                                 }});                        \
    } while (0)

#define Counting_Interceptor(prefix__, type__, n__)                                   \
    do {                                                                              \
        dynamic_cast<Tools::DynamicMemory &>(*storage)                                \
            .add_interceptor(Tools::Interceptor {(prefix__), (type__), [&n = (n__)] { \
                                                     if (n-- <= 0) {                  \
                                                         return special_error();      \
                                                     }                                \
                                                     return Status::ok();             \
                                                 }});                                 \
    } while (0)

static constexpr auto EXPECTATION_MATCHER = "^expectation";

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

#define EXPECT_HAS_VALUE(expr)                                                                                                                                       \
    do {                                                                                                                                                             \
        const auto &expect_has_value_status = (expr);                                                                                                                \
        EXPECT_TRUE(expect_has_value_status.has_value()) << get_status_name(expect_has_value_status.error()) << ": " << expect_has_value_status.error().to_string(); \
    } while (0)

#define ASSERT_HAS_VALUE(expr)                                                                                                                                       \
    do {                                                                                                                                                             \
        const auto &expect_has_value_status = (expr);                                                                                                                \
        ASSERT_TRUE(expect_has_value_status.has_value()) << get_status_name(expect_has_value_status.error()) << ": " << expect_has_value_status.error().to_string(); \
    } while (0)

[[nodiscard]] inline auto expose_message(const Status &s)
{
    EXPECT_TRUE(s.is_ok()) << "Unexpected " << get_status_name(s) << " status: " << s.to_string().data();
    return s.is_ok();
}

class InMemoryTest
{
public:
    static constexpr auto ROOT = "test";
    static constexpr auto PREFIX = "test/";

    InMemoryTest()
        : storage {std::make_unique<Tools::DynamicMemory>()}
    {
        EXPECT_TRUE(expose_message(storage->create_directory(ROOT)));
    }

    virtual ~InMemoryTest() = default;

    [[nodiscard]] auto storage_handle() -> Tools::DynamicMemory &
    {
        return dynamic_cast<Tools::DynamicMemory &>(*storage);
    }

    std::unique_ptr<Env> storage;
};

class OnDiskTest
{
public:
    static constexpr auto ROOT = "/tmp/__calico_test__";
    static constexpr auto PREFIX = "/tmp/__calico_test__/";

    OnDiskTest()
        : storage {std::make_unique<EnvPosix>()}
    {
        std::error_code ignore;
        std::filesystem::remove_all(ROOT, ignore);
        EXPECT_TRUE(expose_message(storage->create_directory(ROOT)));
    }

    virtual ~OnDiskTest()
    {
        std::error_code ignore;
        std::filesystem::remove_all(ROOT, ignore);
    }

    std::unique_ptr<Env> storage;
};

class DisabledWriteAheadLog : public WriteAheadLog
{
public:
    DisabledWriteAheadLog() = default;
    ~DisabledWriteAheadLog() override = default;

    [[nodiscard]] auto flushed_lsn() const -> Id override
    {
        return {std::numeric_limits<Size>::max()};
    }

    [[nodiscard]] auto current_lsn() const -> Id override
    {
        return Id::null();
    }

    [[nodiscard]] auto bytes_written() const -> Size override
    {
        return 0;
    }

    auto log(WalPayloadIn) -> Status override
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
    const Size PAGE_SIZE {0x200};
    const Size FRAME_COUNT {16};

    TestWithPager()
        : scratch(PAGE_SIZE, '\x00'),
          log_scratch(wal_scratch_size(PAGE_SIZE), '\x00')
    {
        Pager *temp;
        EXPECT_OK(Pager::open({
                                  PREFIX,
                                  storage.get(),
                                  &log_scratch,
                                  &wal,
                                  nullptr,
                                  &status,
                                  &commit_lsn,
                                  &in_txn,
                                  FRAME_COUNT,
                                  PAGE_SIZE,
                              },
                              &temp));
        pager.reset(temp);
    }

    std::string log_scratch;
    Status status;
    bool in_txn {};
    Lsn commit_lsn;
    DisabledWriteAheadLog wal;
    std::string scratch;
    std::string collect_scratch;
    std::unique_ptr<Pager> pager;
    Tools::RandomGenerator random {1'024 * 1'024 * 8};
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

namespace TestTools
{

template <class T>
auto get(T &t, const std::string &key, std::string &value) -> Status
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
    if (auto s = get(t, key, val); s.is_ok()) {
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
    auto s = t.put(key, value);
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

inline auto write_file(Env &storage, const std::string &path, Slice in) -> void
{
    Editor *file;
    ASSERT_TRUE(storage.new_editor(path, &file).is_ok());
    ASSERT_TRUE(file->write(in, 0).is_ok());
    delete file;
}

inline auto append_file(Env &storage, const std::string &path, Slice in) -> void
{
    Logger *file;
    ASSERT_TRUE(storage.new_logger(path, &file).is_ok());
    ASSERT_TRUE(file->write(in).is_ok());
    delete file;
}

inline auto read_file(Env &storage, const std::string &path) -> std::string
{
    Reader *file;
    std::string out;
    Size size;

    EXPECT_TRUE(storage.file_size(path, size).is_ok());
    EXPECT_TRUE(storage.new_reader(path, &file).is_ok());
    out.resize(size);

    Span temp {out};
    auto read_size = temp.size();
    EXPECT_TRUE(file->read(temp.data(), read_size, 0).is_ok());
    EXPECT_EQ(read_size, size);
    delete file;
    return out;
}
} // namespace TestTools

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
        Size mean_key_size {12};
        Size mean_value_size {18};
        Size spread {4};
        bool is_sequential {};
        bool is_unique {};
    };

    RecordGenerator() = default;
    explicit RecordGenerator(Parameters);
    auto generate(Tools::RandomGenerator &, Size) const -> std::vector<Record>;

private:
    Parameters m_param;
};

} // namespace calicodb

#endif // CALICODB_TEST_UNIT_TESTS_H
