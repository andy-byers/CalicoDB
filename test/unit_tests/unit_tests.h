#ifndef CALICO_TEST_UNIT_TESTS_H
#define CALICO_TEST_UNIT_TESTS_H

#include "calico/status.h"
#include "pager/page.h"
#include "storage/posix_storage.h"
#include "utils/utils.h"
#include "wal/cleanup.h"
#include "wal/wal.h"
#include "wal/writer.h"
#include "tools.h"
#include <gtest/gtest.h>
#include <iomanip>
#include <sstream>

namespace Calico {

#define Quick_Interceptor(prefix__, type__) \
    do { \
        dynamic_cast<Tools::DynamicMemory &>(*storage) \
            .add_interceptor(Tools::Interceptor {(prefix__), (type__), [] {return special_error();}}); \
    } while (0)

#define Counting_Interceptor(prefix__, type__, n__) \
    do { \
        dynamic_cast<Tools::DynamicMemory &>(*storage) \
            .add_interceptor(Tools::Interceptor {(prefix__), (type__), [&n = (n__)] { \
                if (n-- <= 0) { \
                    return special_error(); \
                } \
                return ok(); \
            }}); \
    } while (0)

static constexpr auto EXPECTATION_MATCHER = "^expectation";

#define EXPECT_OK(expr) \
    do { \
        const auto &expect_ok_status = (expr); \
        EXPECT_TRUE(expect_ok_status.is_ok()) << get_status_name(expect_ok_status) << ": " << expect_ok_status.what().data(); \
    } while (0)

#define ASSERT_OK(expr) \
    do { \
        const auto &expect_ok_status = (expr); \
        ASSERT_TRUE(expect_ok_status.is_ok()) << get_status_name(expect_ok_status) << ": " << expect_ok_status.what().data(); \
    } while (0)


[[nodiscard]]
inline auto expose_message(const Status &s)
{
    EXPECT_TRUE(s.is_ok()) << "Unexpected " << get_status_name(s) << " status: " << s.what().data();
    return s.is_ok();
}

class InMemoryTest : public testing::Test {
public:
    static constexpr auto ROOT = "test";
    static constexpr auto PREFIX = "test/";

    InMemoryTest()
        : storage {std::make_unique<Tools::DynamicMemory>()}
    {
        CALICO_EXPECT_TRUE(expose_message(storage->create_directory(ROOT)));
    }

    ~InMemoryTest() override = default;

    [[nodiscard]]
    auto storage_handle() -> Tools::DynamicMemory &
    {
        return dynamic_cast<Tools::DynamicMemory &>(*storage);
    }

    std::unique_ptr<Storage> storage;
};

template<class ...Param>
class ParameterizedInMemoryTest : public testing::TestWithParam<Param...> {
public:
    static constexpr auto ROOT = "test";
    static constexpr auto PREFIX = "test/";

    ParameterizedInMemoryTest()
        : storage {std::make_unique<Tools::DynamicMemory>()}
    {
        CALICO_EXPECT_TRUE(expose_message(storage->create_directory(ROOT)));
    }

    ~ParameterizedInMemoryTest() override = default;

    [[nodiscard]]
    auto storage_handle() -> Tools::DynamicMemory &
    {
        return dynamic_cast<Tools::DynamicMemory &>(*storage);
    }

    std::unique_ptr<Storage> storage;
};

class OnDiskTest : public testing::Test {
public:
    static constexpr auto ROOT = "/tmp/__calico_test__";
    static constexpr auto PREFIX = "/tmp/__calico_test__/";

    OnDiskTest()
        : storage {std::make_unique<PosixStorage>()}
    {
        std::error_code ignore;
        std::filesystem::remove_all(ROOT, ignore);
        CALICO_EXPECT_TRUE(expose_message(storage->create_directory(ROOT)));
    }

    ~OnDiskTest() override
    {
        std::error_code ignore;
        std::filesystem::remove_all(ROOT, ignore);
    }

    std::unique_ptr<Storage> storage;
};

template<class ...Param>
class ParameterizedOnDiskTest : public testing::TestWithParam<Param...> {
public:
    static constexpr auto ROOT = "/tmp/__calico_test__";
    static constexpr auto PREFIX = "/tmp/__calico_test__/";

    ParameterizedOnDiskTest()
        : storage {std::make_unique<PosixStorage>()}
    {
        std::error_code ignore;
        std::filesystem::remove_all(ROOT, ignore);
        CALICO_EXPECT_TRUE(expose_message(storage->create_directory(ROOT)));
    }

    ~ParameterizedOnDiskTest() override
    {
        std::error_code ignore;
        std::filesystem::remove_all(ROOT, ignore);
    }

    std::unique_ptr<Storage> storage;
};

class DisabledWriteAheadLog: public WriteAheadLog {
public:
    DisabledWriteAheadLog() = default;
    ~DisabledWriteAheadLog() override = default;

    [[nodiscard]]
    auto flushed_lsn() const -> Id override
    {
        return Id::null();
    }

    [[nodiscard]]
    auto current_lsn() const -> Id override
    {
        return Id::null();
    }

    [[nodiscard]]
    auto bytes_written() const -> Size override
    {
        return 0;
    }

    auto log(WalPayloadIn) -> void override
    {

    }

    auto flush() -> Status override
    {
        return ok();
    }

    auto advance() -> Status override
    {
        return ok();
    }

    [[nodiscard]]
    auto roll_forward(Id, const Callback &) -> Status override
    {
        return ok();
    }

    [[nodiscard]]
    auto roll_backward(Id, const Callback &) -> Status override
    {
        return ok();
    }

    auto cleanup(Id) -> void override
    {

    }

    [[nodiscard]] auto start_workers() -> Status override
    {
        return ok();
    }

    [[nodiscard]]
    auto truncate(Id) -> Status override
    {
        return ok();
    }
};

inline auto expect_ok(const Status &s) -> void
{
    if (!s.is_ok()) {
        fmt::print(stderr, "unexpected {} status: {}\n", get_status_name(s), s.what().data());
        std::abort();
    }
}

[[nodiscard]]
inline auto special_error()
{
    return system_error("42");
}

inline auto assert_special_error(const Status &s)
{
    if (!s.is_system_error() || s.what() != special_error().what()) {
        fmt::print(stderr, "error: unexpected {} status: {}", get_status_name(s), s.is_ok() ? "NULL" : s.what().to_string());
        std::exit(EXIT_FAILURE);
    }
}

namespace TestTools {

    template<class T>
    auto get(T &t, const std::string &key, std::string &value) -> Status
    {
        return t.get(key, value);
    }

    template<class T>
    auto find(T &t, const std::string &key) -> Cursor *
    {
        auto *cursor = t.new_cursor();
        if (cursor) {
            cursor->seek(key);
        }
        return cursor;
    }

    template<class T>
    auto contains(T &t, const std::string &key) -> bool
    {
        std::string value;
        return get(t, key, value).is_ok();
    }

    template<class T>
    auto contains(T &t, const std::string &key, const std::string &value) -> bool
    {
        std::string val;
        if (auto s = get(t, key, val); s.is_ok()) {
            return val == value;
        }
        return false;
    }

    template<class T>
    auto expect_contains(T &t, const std::string &key, const std::string &value) -> void
    {
        std::string val;
        const auto MSG = fmt::format("expected record ({}, {})\n", key, value);
        if (auto s = get(t, key, val); s.is_ok()) {
            if (val != value) {
                fmt::print(stderr, "{}: value \"{}\" does not match\n", MSG, val);
                std::exit(EXIT_FAILURE);
            }
        } else {
            fmt::print(stderr, "{}: {}\n", MSG, "could not find key");
            std::exit(EXIT_FAILURE);
        }
    }

    template<class T>
    auto insert(T &t, const std::string &key, const std::string &value) -> void
    {
        auto s = t.put(key, value);
        if (!s.is_ok()) {
            fmt::print(stderr, "error: {}\n", s.what().data());
            CALICO_EXPECT_TRUE(false && "Error: insert() failed");
        }
    }

    template<class T>
    auto erase(T &t, const std::string &key) -> bool
    {
        auto s = t.erase(get(t, key));
        if (!s.is_ok() && !s.is_not_found()) {
            fmt::print(stderr, "error: {}\n", s.what().data());
            CALICO_EXPECT_TRUE(false && "Error: erase() failed");
        }
        return !s.is_not_found();
    }

    template<class T>
    auto erase_one(T &t, const std::string &key) -> bool
    {
        auto was_erased = t.erase(get(t, key));
        CALICO_EXPECT_TRUE(was_erased.has_value());
        if (was_erased.value())
            return true;
        auto cursor = t.first();
        CALICO_EXPECT_EQ(cursor.error(), std::nullopt);
        if (!cursor.is_valid())
            return false;
        was_erased = t.erase(cursor);
        CALICO_EXPECT_TRUE(was_erased.value());
        return true;
    }

    inline auto write_file(Storage &storage, const std::string &path, Slice in) -> void
    {
        RandomEditor *file;
        CALICO_EXPECT_TRUE(storage.open_random_editor(path, &file).is_ok());
        CALICO_EXPECT_TRUE(file->write(in, 0).is_ok());
        delete file;
    }

    inline auto append_file(Storage &storage, const std::string &path, Slice in) -> void
    {
        AppendWriter *file;
        CALICO_EXPECT_TRUE(storage.open_append_writer(path, &file).is_ok());
        CALICO_EXPECT_TRUE(file->write(in).is_ok());
        delete file;
    }

    inline auto read_file(Storage &storage, const std::string &path) -> std::string
    {
        RandomReader *file;
        std::string out;
        Size size;

        CALICO_EXPECT_TRUE(storage.file_size(path, size).is_ok());
        CALICO_EXPECT_TRUE(storage.open_random_reader(path, &file).is_ok());
        out.resize(size);

        Span temp {out};
        auto read_size = temp.size();
        CALICO_EXPECT_TRUE(file->read(temp.data(), read_size, 0).is_ok());
        CALICO_EXPECT_EQ(read_size, size);
        delete file;
        return out;
    }

    [[nodiscard]]
    inline auto snapshot(Storage &storage, Size page_size) -> std::string
    {
        static constexpr Size CODE {0x1234567887654321};

        Size file_size;
        CALICO_EXPECT_TRUE(storage.file_size("test/data", file_size).is_ok());

        std::unique_ptr<RandomReader> reader;
        {
            RandomReader *temp;
            expect_ok(storage.open_random_reader("test/data", &temp));
            reader.reset(temp);
        }

        std::string buffer(file_size, '\x00');
        auto read_size = file_size;
        expect_ok(reader->read(buffer.data(), read_size, 0));
        CALICO_EXPECT_EQ(read_size, file_size);

        CALICO_EXPECT_EQ(file_size % page_size, 0);

        auto offset = FileHeader::SIZE;
        for (Size i {}; i < file_size / page_size; ++i) {
            put_u64(buffer.data() + i*page_size + offset, CODE);
            offset = 0;
        }

        // Clear header fields that might be inconsistent, despite identical database contents.
        Page root {Id::root(),{buffer.data(), page_size}, true};
        FileHeader header {root};
        header.header_crc = 0;
        header.recovery_lsn.value = CODE;
        header.write(root);

        return buffer;
    }
} // UnitTests

struct Record {
    inline auto operator<(const Record &rhs) const -> bool
    {
        return Slice {key} < Slice {rhs.key};
    }

    std::string key;
    std::string value;
};

auto operator>(const Record&, const Record&) -> bool;
auto operator<=(const Record&, const Record&) -> bool;
auto operator>=(const Record&, const Record&) -> bool;
auto operator==(const Record&, const Record&) -> bool;
auto operator!=(const Record&, const Record&) -> bool;

class RecordGenerator {
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

} // namespace Calico

#endif // CALICO_TEST_UNIT_TESTS_H
