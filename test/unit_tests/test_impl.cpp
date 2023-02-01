
#include "core/database_impl.h"
#include "fakes.h"
#include "storage/posix_storage.h"
#include "tools.h"
#include "tree/cursor_internal.h"
#include "tree/header.h"
#include "tree/tree.h"
#include "unit_tests.h"
#include "wal/wal.h"
#include <array>
#include <filesystem>
#include <gtest/gtest.h>
#include <vector>

namespace Calico {

namespace UnitTests {
    extern std::uint32_t random_seed;
} // namespace internal

namespace fs = std::filesystem;

class DatabaseOpenTests: public TestOnDisk {};

TEST_F(DatabaseOpenTests, MaximumPageSize)
{
    Options options;
    options.page_size = MAXIMUM_PAGE_SIZE;
    options.page_cache_size = options.page_size * 64;
    options.wal_buffer_size = options.page_size * 64;

    for (Size i {}; i < 2; ++i) {
        Database db;
        ASSERT_OK(db.open(ROOT, options));
        ASSERT_EQ(db.statistics().page_size(), MAXIMUM_PAGE_SIZE);
        ASSERT_OK(db.close());
    }
}

class BasicDatabaseTests: public TestOnDisk {
public:
    BasicDatabaseTests()
    {
        options.page_size = 0x200;
        options.page_cache_size = options.page_size * frame_count;
        options.wal_buffer_size = options.page_cache_size;
        options.log_level = LogLevel::OFF;
        options.storage = storage.get();
    }

    ~BasicDatabaseTests() override = default;

    Size frame_count {64};
    Options options;
};

TEST_F(BasicDatabaseTests, OpensAndCloses)
{
    Database db;
    for (Size i {}; i < 10; ++i) {
        ASSERT_OK(db.open(ROOT, options));
        ASSERT_OK(db.close());
    }
    ASSERT_TRUE(storage->file_exists(std::string {PREFIX} + "data").is_ok());
}

TEST_F(BasicDatabaseTests, IsDestroyed)
{
    const auto filename = std::string {PREFIX} + "data";

    Database db;
    ASSERT_OK(db.open(ROOT, options));
    ASSERT_TRUE(storage->file_exists(filename).is_ok());
    ASSERT_OK(std::move(db).destroy());
    ASSERT_TRUE(storage->file_exists(filename).is_not_found());
}

static auto insert_random_groups(Database &db, Size num_groups, Size group_size)
{
    RecordGenerator generator;
    Random random {UnitTests::random_seed};

    for (Size iteration {}; iteration < num_groups; ++iteration) {
        const auto records = generator.generate(random, group_size);
        auto itr = cbegin(records);
        ASSERT_OK(db.status());
        auto xact = db.start();

        for (Size i {}; i < group_size; ++i) {
            ASSERT_OK(db.put(itr->key, itr->value));
            itr++;
        }
        ASSERT_OK(xact.commit());
    }
}

static auto traverse_all_records(Database &db)
{
    auto c = db.cursor();
    c.seek_first();
    for (; c.is_valid(); c.next()) {
        CursorInternal::TEST_validate(c);
    }
    c.seek_last();
    for (; c.is_valid(); c.previous()) {
        CursorInternal::TEST_validate(c);
    }
}

TEST_F(BasicDatabaseTests, InsertOneGroup)
{
    Database db;
    ASSERT_OK(db.open(ROOT, options));
    insert_random_groups(db, 1, 500);
    traverse_all_records(db);
    ASSERT_OK(db.close());
}

TEST_F(BasicDatabaseTests, InsertMultipleGroups)
{
    Database db;
    ASSERT_OK(db.open(ROOT, options));
    insert_random_groups(db, 10, 500);
    traverse_all_records(db);
    ASSERT_OK(db.close());
}

TEST_F(BasicDatabaseTests, DataPersists)
{
    static constexpr Size NUM_ITERATIONS {5};
    static constexpr Size GROUP_SIZE {10};

    auto s = ok();
    RecordGenerator generator;
    Random random {UnitTests::random_seed};

    const auto records = generator.generate(random, GROUP_SIZE * NUM_ITERATIONS);
    auto itr = cbegin(records);
    Database db;

    for (Size iteration {}; iteration < NUM_ITERATIONS; ++iteration) {
        ASSERT_OK(db.open(ROOT, options));
        ASSERT_OK(db.status());
        auto xact = db.start();

        for (Size i {}; i < GROUP_SIZE; ++i) {
            ASSERT_OK(db.put(itr->key, itr->value));
            itr++;
        }
        ASSERT_OK(xact.commit());
        ASSERT_OK(db.close());
    }

    ASSERT_OK(db.open(ROOT, options));
    CALICO_EXPECT_EQ(db.statistics().record_count(), records.size());
    for (const auto &[key, value]: records) {
        std::string value_out;
        const auto s = tools::get(db, key, value_out);
        ASSERT_TRUE(s.is_ok());
        ASSERT_EQ(value_out, value);
    }
    ASSERT_TRUE(db.close().is_ok());
}

TEST_F(BasicDatabaseTests, ReportsInvalidPageSizes)
{
    auto invalid = options;

    Database db;
    invalid.page_size = MINIMUM_PAGE_SIZE / 2;
    ASSERT_TRUE(db.open(ROOT, invalid).is_invalid_argument());

    invalid.page_size = MAXIMUM_PAGE_SIZE * 2;
    ASSERT_TRUE(db.open(ROOT, invalid).is_invalid_argument());

    invalid.page_size = DEFAULT_PAGE_SIZE - 1;
    ASSERT_TRUE(db.open(ROOT, invalid).is_invalid_argument());
}

class ReaderTests: public BasicDatabaseTests {
public:
    static constexpr Size KEY_WIDTH {6};
    static constexpr Size NUM_RECORDS {200};

    auto SetUp() -> void override
    {
        ASSERT_OK(db.open(ROOT, options));

        auto xact = db.start();
        for (Size i {}; i < NUM_RECORDS; ++i) {
            const auto key = make_key<KEY_WIDTH>(i);
            ASSERT_OK(db.put(key, key));
        }
        ASSERT_OK(xact.commit());
    }

    auto TearDown() -> void override
    {
        ASSERT_OK(db.close());
    }

    auto localized_reader() const -> void
    {
        static constexpr Size NUM_ROUNDS {2};

        // Concentrate the cursors on the first N records.
        static constexpr Size N {10};
        static_assert(NUM_RECORDS >= N);

        for (Size i {}; i < NUM_ROUNDS; ++i) {
            Size counter {};
            auto c = db.cursor();
            c.seek_first();
            for (; counter < N; c.next(), ++counter) {
                const auto key = make_key<KEY_WIDTH>(counter);
                ASSERT_EQ(c.key().to_string(), key);
                ASSERT_EQ(c.value(), key);
            }
        }
    }

    auto distributed_reader(Size r) const -> void
    {
        static constexpr Size MAX_ROUND_SIZE {10};
        // Try to spread the cursors out across the database.
        const auto first = r * NUM_RECORDS % NUM_RECORDS;
        for (auto i = first; i < NUM_RECORDS; ++i) {
            auto c = db.cursor();
            c.seek(make_key<KEY_WIDTH>(i));

            for (auto j = i; j < std::min(i + MAX_ROUND_SIZE, NUM_RECORDS); ++j, c.next()) {
                const auto key = make_key<KEY_WIDTH>(j);
                ASSERT_TRUE(c.is_valid());
                ASSERT_EQ(c.key().to_string(), key);
                ASSERT_EQ(c.value(), key);
            }
        }
    }

    Random random {UnitTests::random_seed};
    Database db;
};

TEST_F(ReaderTests, SingleReader)
{
    for (Size x {}; x < 1'000; ++x) {
        std::vector<std::string> strings;
        for (Size i {}; i < NUM_RECORDS; ++i) {
            auto c = db.cursor();
            c.seek(make_key<KEY_WIDTH>(i));
            ASSERT_TRUE(c.is_valid());
            strings.emplace_back(c.value().to_string());
        }
    }
    distributed_reader(0);
    localized_reader();
}

TEST_F(ReaderTests, DistributedReaders)
{
    std::vector<std::thread> readers;
    for (Size i {}; i < frame_count * 2; ++i) {
        readers.emplace_back(std::thread {[this, i] {
            distributed_reader(i);
        }});
    }
    for (auto &reader: readers) {
        reader.join();
    }
}

TEST_F(ReaderTests, LocalizedReaders)
{
    std::vector<std::thread> readers;
    for (Size i {}; i < frame_count * 2; ++i) {
        readers.emplace_back(std::thread {[this] {
            localized_reader();
        }});
    }
    for (auto &reader: readers)
        reader.join();
}

class TestDatabase {
public:
    explicit TestDatabase(Storage &storage)
    {
        options.page_size = 0x200;
        options.page_cache_size = 32 * options.page_size;
        options.wal_buffer_size = 32 * options.page_size;
        options.storage = &storage;

        impl = std::make_unique<DatabaseImpl>();
        const auto s = impl->open("test", options);
        EXPECT_TRUE(s.is_ok()) << "Error: " << s.what().to_string();
    }

    ~TestDatabase() = default;

    [[nodiscard]]
    auto snapshot() const -> std::string
    {
        return tools::snapshot(*options.storage, options.page_size);
    }

    Options options;
    Random random {UnitTests::random_seed};
    std::vector<Record> records;
    std::unique_ptr<DatabaseImpl> impl;
};

class DbAbortTests: public testing::Test {
protected:
    DbAbortTests()
    {
        storage = std::make_unique<HeapStorage>();
        EXPECT_OK(storage->create_directory("test"));
        db = std::make_unique<TestDatabase>(*storage);
    }
    ~DbAbortTests() override = default;

    std::unique_ptr<Storage> storage;
    std::unique_ptr<TestDatabase> db;
};

static auto add_records(TestDatabase &test, Size n, Size max_value_size, const std::string &prefix = {})
{
    std::vector<Record> records(n);

    for (Size i {}; i < n; ++i) {
        const auto value_size = test.random.get(max_value_size);
        records[i].key = prefix + make_key(i);
        records[i].value = test.random.get<std::string>('a', 'z', value_size);
        EXPECT_OK(test.impl->put(records[i].key, records[i].value));
    }
    return records;
}

TEST_F(DbAbortTests, RevertsFirstBatch)
{
    const auto snapshot = db->snapshot();
    auto xact = db->impl->start();
    add_records(*db, 3, 0x400);
    ASSERT_OK(xact.abort());
    ASSERT_EQ(snapshot, db->snapshot());
}

TEST_F(DbAbortTests, RevertsSecondBatch)
{
    auto committed = db->impl->start();
    add_records(*db, 3, 0x400, "_");
    ASSERT_OK(committed.commit());

    // Hack to make sure the database file is up-to-date.
    (void)db->impl->pager->flush({});

    const auto snapshot = db->snapshot();
    auto xact = db->impl->start();
    add_records(*db, 3, 0x400);
    ASSERT_OK(xact.abort());
    ASSERT_EQ(snapshot, db->snapshot());
}

TEST_F(DbAbortTests, RevertsNthBatch)
{
    for (Size i {}; i < 10; ++i) {
        auto xact = db->impl->start();
        add_records(*db, 100, 0x400, "_");
        ASSERT_OK(xact.commit());
    }
    // Hack to make sure the database file is up-to-date.
    (void)db->impl->pager->flush({});

    const auto snapshot = db->snapshot();
    auto xact = db->impl->start();
    add_records(*db, 1'000, 0x400);
    ASSERT_OK(xact.abort());
    ASSERT_EQ(snapshot, db->snapshot());
}

class DbRecoveryTests: public testing::Test {
protected:
    DbRecoveryTests()
    {
        storage = std::make_unique<HeapStorage>();
        EXPECT_OK(storage->create_directory("test"));
    }
    ~DbRecoveryTests() override = default;

    std::unique_ptr<Storage> storage;
};

TEST_F(DbRecoveryTests, RecoversFirstBatch)
{
    std::unique_ptr<Storage> clone;
    std::string snapshot;

    {
        TestDatabase db {*storage};
        auto xact = db.impl->start();
        add_records(db, 100, 10 * db.options.page_size);
        ASSERT_OK(xact.commit());

        // Simulate a crash by cloning the database before cleanup has occurred.
        clone.reset(dynamic_cast<const HeapStorage &>(*storage).clone());

        (void)db.impl->pager->flush({});
        snapshot = db.snapshot();
    }
    // Create a new database from the cloned data. This database will need to roll the WAL forward to become
    // consistent.
    ASSERT_EQ(snapshot, TestDatabase {*clone}.snapshot());
}

TEST_F(DbRecoveryTests, RecoversNthBatch)
{
    std::unique_ptr<Storage> clone;
    std::string snapshot;

    {
        TestDatabase db {*storage};

        for (Size i {}; i < 10; ++i) {
            auto xact = db.impl->start();
            add_records(db, 100, 10 * db.options.page_size);
            ASSERT_OK(xact.commit());
        }

        clone.reset(dynamic_cast<const HeapStorage &>(*storage).clone());

        (void)db.impl->pager->flush({});
        snapshot = db.snapshot();
    }
    ASSERT_EQ(snapshot, TestDatabase {*clone}.snapshot());
}

enum class ErrorTarget {
    DATA_WRITE,
    DATA_READ,
    WAL_WRITE,
    WAL_READ,
};

struct ErrorWrapper {
    ErrorTarget target {};
    Size successes {};
};

class DbErrorTests: public testing::TestWithParam<ErrorWrapper> {
protected:
    DbErrorTests()
    {
        storage = std::make_unique<HeapStorage>();
        EXPECT_OK(storage->create_directory("test"));
        db = std::make_unique<TestDatabase>(*storage);

        auto xact = db->impl->start();
        committed = add_records(*db, 5'000, 10);
        EXPECT_OK(xact.commit());

        interceptors::set_read([this](const auto &prefix, ...) {
            if (prefix == "test/data") {
                if (counter++ >= GetParam().successes) {
                    return special_error();
                }
            }
            return ok();
        });
    }
    ~DbErrorTests() override = default;

    std::unique_ptr<Storage> storage;
    std::unique_ptr<TestDatabase> db;
    std::vector<Record> committed;
    Size counter {};
};

TEST_P(DbErrorTests, HandlesReadErrorDuringQuery)
{
    const auto stat = db->impl->statistics();

    for (Size iteration {}; iteration < 2; ++iteration) {
        for (Size i {}, n = stat.record_count(); i < n; ++i) {
            std::string value;
            const auto s = db->impl->get(make_key(i), value);

            if (!s.is_ok()) {
                assert_special_error(s);
                break;
            }
        }
        ASSERT_OK(db->impl->status());
        counter = 0;
    }
}

TEST_P(DbErrorTests, HandlesReadErrorDuringIteration)
{
    auto cursor = db->impl->cursor();
    cursor.seek_first();
    while (cursor.is_valid()) {
        (void)cursor.key();
        (void)cursor.value();
        cursor.next();
    }
    assert_special_error(cursor.status());
    ASSERT_OK(db->impl->status());
    counter = 0;

    cursor.seek_last();
    while (cursor.is_valid()) {
        (void)cursor.key();
        (void)cursor.value();
        cursor.previous();
    }
    assert_special_error(cursor.status());
    ASSERT_OK(db->impl->status());
}

INSTANTIATE_TEST_SUITE_P(
    DbErrorTests,
    DbErrorTests,
    ::testing::Values(
        ErrorWrapper {{}, 0},
        ErrorWrapper {{}, 1},
        ErrorWrapper {{}, 10},
        ErrorWrapper {{}, 100}));

class DbFatalErrorTests: public testing::TestWithParam<ErrorWrapper> {
protected:
    DbFatalErrorTests()
    {
        storage = std::make_unique<HeapStorage>();
        EXPECT_OK(storage->create_directory("test"));
        db = std::make_unique<TestDatabase>(*storage);

        auto xact = db->impl->start();
        committed = add_records(*db, 5'000, 10);
        EXPECT_OK(xact.commit());

        const auto make_interceptor = [this](const auto &prefix) {
            return [this, prefix](const auto &filename, ...) {
                if (Slice {filename}.starts_with(prefix)) {
                    if (counter++ == GetParam().successes) {
                        return special_error();
                    }
                }
                return ok();
            };
        };

        switch (GetParam().target) {
            case ErrorTarget::DATA_READ:
                interceptors::set_read(make_interceptor("test/data"));
                break;
            case ErrorTarget::DATA_WRITE:
                interceptors::set_write(make_interceptor("test/data"));
                break;
            case ErrorTarget::WAL_READ:
                interceptors::set_read(make_interceptor("test/wal"));
                break;
            case ErrorTarget::WAL_WRITE:
                interceptors::set_write(make_interceptor("test/wal"));
                break;
        }
    }

    ~DbFatalErrorTests() override = default;

    std::unique_ptr<Storage> storage;
    std::unique_ptr<TestDatabase> db;
    std::vector<Record> committed;
    Size counter {};
};

TEST_P(DbFatalErrorTests, ErrorsDuringModificationsAreFatal)
{
    const auto count = db->impl->statistics().record_count();

    while (db->impl->status().is_ok()) {
        for (Size i {}; i < count && db->impl->erase(make_key(i)).is_ok(); ++i) {

        }
        for (Size i {}; i < count && db->impl->put(make_key(i), "value").is_ok(); ++i) {

        }
    }
    assert_special_error(db->impl->status());
    assert_special_error(db->impl->put("key", "value"));
}

TEST_P(DbFatalErrorTests, RecoversFromFatalErrors)
{
    const auto count = db->impl->statistics().record_count();

    auto xact = db->impl->start();
    Size i {};
    while (i < count && db->impl->erase(make_key(i++)).is_ok()) {

    }
    assert_special_error(db->impl->status());
    assert_special_error(xact.commit());
    assert_special_error(db->impl->put("key", "value"));
    assert_special_error(db->impl->close());

    ASSERT_OK(db->impl->open("test", db->options));
    for (const auto &[key, value]: committed) {
        tools::expect_contains(*db->impl, key, value);
    }
    ASSERT_EQ(db->impl->statistics().record_count(), committed.size());
}

INSTANTIATE_TEST_SUITE_P(
    DbFatalErrorTests,
    DbFatalErrorTests,
    ::testing::Values(
        ErrorWrapper {ErrorTarget::DATA_READ, 0},
        ErrorWrapper {ErrorTarget::DATA_READ, 1},
        ErrorWrapper {ErrorTarget::DATA_READ, 10},
        ErrorWrapper {ErrorTarget::DATA_READ, 100},
        ErrorWrapper {ErrorTarget::DATA_WRITE, 0},
        ErrorWrapper {ErrorTarget::DATA_WRITE, 1},
        ErrorWrapper {ErrorTarget::DATA_WRITE, 10},
        ErrorWrapper {ErrorTarget::DATA_WRITE, 100},
        ErrorWrapper {ErrorTarget::WAL_WRITE, 0},
        ErrorWrapper {ErrorTarget::WAL_WRITE, 1},
        ErrorWrapper {ErrorTarget::WAL_WRITE, 10},
        ErrorWrapper {ErrorTarget::WAL_WRITE, 100}));

} // <anonymous>
