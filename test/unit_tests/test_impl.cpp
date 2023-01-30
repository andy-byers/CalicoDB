
#include "core/database_impl.h"
#include "fakes.h"
#include "storage/posix_storage.h"
#include "tools.h"
#include "tree/bplus_tree.h"
#include "tree/cursor_internal.h"
#include "tree/header.h"
#include "unit_tests.h"
#include "wal/basic_wal.h"
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
        options.storage = store.get();
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
    ASSERT_TRUE(store->file_exists(std::string {PREFIX} + "data").is_ok());
}

TEST_F(BasicDatabaseTests, IsDestroyed)
{
    const auto filename = std::string {PREFIX} + "data";

    Database db;
    ASSERT_OK(db.open(ROOT, options));
    ASSERT_TRUE(store->file_exists(filename).is_ok());
    ASSERT_OK(std::move(db).destroy());
    ASSERT_TRUE(store->file_exists(filename).is_not_found());
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

// TODO: It would be nice to have better parallelism in the pager for multiple readers. Currently, we lock a mutex around all pager operations,
//       causing a lot of contention. Maybe we could use some kind of per-frame locks?
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

    auto distributed_reader(Size r) -> void
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

TEST_F(ReaderTests, ManyDistributedReaders)
{
    std::vector<std::thread> readers;
    for (Size i {}; i < frame_count * 2; ++i)
        readers.emplace_back(std::thread {[this, i] {distributed_reader(i);}});
    for (auto &reader: readers)
        reader.join();
}

TEST_F(ReaderTests, ManyLocalizedReaders)
{
    std::vector<std::thread> readers;
    for (Size i {}; i < frame_count * 2; ++i)
        readers.emplace_back(std::thread {[this] {localized_reader();}});
    for (auto &reader: readers)
        reader.join();
}

class TestDatabase {
public:
    static constexpr Size CODE {0x1234567887654321};

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
    auto time_independent_snapshot() const -> std::string
    {
        auto &store = *options.storage;

        Size file_size;
        EXPECT_OK(store.file_size("test/data", file_size));

        std::unique_ptr<RandomReader> reader;
        {
            RandomReader *temp;
            EXPECT_OK(store.open_random_reader("test/data", &temp));
            reader.reset(temp);
        }

        std::string buffer(file_size, '\x00');
        auto read_size = file_size;
        EXPECT_OK(reader->read(buffer.data(), read_size, 0));
        EXPECT_EQ(read_size, file_size);

        const auto stat = impl->statistics();
        EXPECT_EQ(file_size % stat.page_size(), 0);

        auto offset = FileHeader::SIZE;
        for (Size i {}; i < file_size / stat.page_size(); ++i) {
            put_u64(buffer.data() + i*stat.page_size() + offset, CODE);
            offset = 0;
        }

        // Clear header fields that might be inconsistent, despite identical database contents.
        Page root {Id::root(),{buffer.data(), stat.page_size()}, true};
        FileHeader header {root};
        header.header_crc = 0;
        header.recovery_lsn.value = CODE;
        header.write(root);

        return buffer;
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

TEST_F(DbAbortTests, RevertsEmbeddedRecords)
{
    const auto snapshot = db->time_independent_snapshot();
    auto xact = db->impl->start();
    add_records(*db, 3, 10);
    ASSERT_OK(xact.abort());
    ASSERT_EQ(snapshot, db->time_independent_snapshot());
}

TEST_F(DbAbortTests, RevertsOverflowPages)
{
    const auto snapshot = db->time_independent_snapshot();
    auto xact = db->impl->start();
    add_records(*db, 3, 10 * db->options.page_size);
    ASSERT_OK(xact.abort());
    ASSERT_EQ(snapshot, db->time_independent_snapshot());
}

TEST_F(DbAbortTests, RevertsSecondBatchOfEmbeddedRecords)
{
    auto committed = db->impl->start();
    add_records(*db, 3, 10, "_");
    ASSERT_OK(committed.commit());

    // Hack to make sure the database file is up-to-date.
    (void)db->impl->pager->flush({});

    const auto snapshot = db->time_independent_snapshot();
    auto xact = db->impl->start();
    add_records(*db, 3, 10);
    ASSERT_OK(xact.abort());
    ASSERT_EQ(snapshot, db->time_independent_snapshot());
}

TEST_F(DbAbortTests, RevertsSecondBatchOfOverflowPages)
{
    auto committed = db->impl->start();
    add_records(*db, 3, 10 * db->options.page_size, "_");
    ASSERT_OK(committed.commit());

    // Hack to make sure the database file is up-to-date.
    (void)db->impl->pager->flush({});

    const auto snapshot = db->time_independent_snapshot();
    auto xact = db->impl->start();
    add_records(*db, 3, 10 * db->options.page_size);
    ASSERT_OK(xact.abort());
    ASSERT_EQ(snapshot, db->time_independent_snapshot());
}

TEST_F(DbAbortTests, RevertsNthBatchOfEmbeddedRecords)
{
    // Don't explicitly use a transaction. This causes 100 single-insert transactions to be run.
    add_records(*db, 100, 10, "_");

    // Hack to make sure the database file is up-to-date.
    (void)db->impl->pager->flush({});

    const auto snapshot = db->time_independent_snapshot();
    auto xact = db->impl->start();
    add_records(*db, 1'000, 10);
    ASSERT_OK(xact.abort());
    ASSERT_EQ(snapshot, db->time_independent_snapshot());
}

TEST_F(DbAbortTests, RevertsNthBatchOfOverflowPages)
{
    add_records(*db, 100, 10 * db->options.page_size, "_");

    // Hack to make sure the database file is up-to-date.
    (void)db->impl->pager->flush({});

    const auto snapshot = db->time_independent_snapshot();
    auto xact = db->impl->start();
    add_records(*db, 1'000, 10 * db->options.page_size);
    ASSERT_OK(xact.abort());
    ASSERT_EQ(snapshot, db->time_independent_snapshot());
}

class DbRecoveryTests: public testing::Test {
protected:
    DbRecoveryTests()
    {
        store = std::make_unique<HeapStorage>();
        EXPECT_OK(store->create_directory("test"));
    }
    ~DbRecoveryTests() override = default;

    std::unique_ptr<Storage> store;
};

TEST_F(DbRecoveryTests, RecoversFirstBatch)
{
    std::unique_ptr<Storage> clone;
    std::string snapshot;

    {
        TestDatabase db {*store};
        auto xact = db.impl->start();
        add_records(db, 100, 10 * db.options.page_size);
        ASSERT_OK(xact.commit());

        // Simulate a crash by cloning the database before cleanup has occurred.
        clone.reset(dynamic_cast<const HeapStorage &>(*store).clone());

        (void)db.impl->pager->flush({});
        snapshot = db.time_independent_snapshot();
    }
    // Create a new database from the cloned data. This database will need to roll the WAL forward to become
    // consistent.
    ASSERT_EQ(snapshot, TestDatabase {*clone}.time_independent_snapshot());
}

TEST_F(DbRecoveryTests, RecoversNthBatch)
{
    std::unique_ptr<Storage> clone;
    std::string snapshot;

    {
        TestDatabase db {*store};

        for (Size i {}; i < 10; ++i) {
            auto xact = db.impl->start();
            add_records(db, 100, 10 * db.options.page_size);
            ASSERT_OK(xact.commit());
        }

        clone.reset(dynamic_cast<const HeapStorage &>(*store).clone());

        (void)db.impl->pager->flush({});
        snapshot = db.time_independent_snapshot();
    }
    ASSERT_EQ(snapshot, TestDatabase {*clone}.time_independent_snapshot());
}

} // <anonymous>
