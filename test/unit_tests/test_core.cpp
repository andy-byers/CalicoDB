
#include "core/core.h"
#include "fakes.h"
#include "storage/posix_storage.h"
#include "tools.h"
#include "tree/bplus_tree.h"
#include "tree/cursor_internal.h"
#include "tree/tree.h"
#include "unit_tests.h"
#include "utils/header.h"
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

template<class T>
constexpr auto is_pod = std::is_standard_layout_v<T> && std::is_trivial_v<T>;

TEST(TestFileHeader, FileHeaderIsPOD)
{
    ASSERT_TRUE(is_pod<FileHeader__>);
}

class TestDatabase {
public:
    TestDatabase()
    {
        Options options;
        options.page_size = 0x200;
        options.page_cache_size = 32 * options.page_size;
        options.wal_buffer_size = 32 * options.page_size;

        store = std::make_unique<HeapStorage>();
        core = std::make_unique<Core>();
        const auto s = core->open("test", options);
        EXPECT_TRUE(s.is_ok()) << "Error: " << s.what().to_string();

//        RecordGenerator::Parameters generator_param;
//        generator_param.mean_key_size = 20;
//        generator_param.mean_value_size = 50;
//        generator_param.spread = 15;
//        auto generator = RecordGenerator {generator_param};
//
//        records = generator.generate(random, 1'500);
//        for (const auto &[key, value]: records)
//            tools::insert(*core, key, value);
//        std::sort(begin(records), end(records));
    }

    ~TestDatabase() = default;

    auto erase_one(const std::string &maybe) -> void
    {
        ASSERT_GT(core->statistics().record_count(), 0);
        auto c = core->find(maybe);

        if (!c.is_valid())
            c = core->first();

        ASSERT_TRUE(c.is_valid());
        ASSERT_OK(core->erase(c.key()));
    }

    Random random {UnitTests::random_seed};
    std::unique_ptr<Storage> store;
    std::vector<Record> records;
    std::unique_ptr<Core> core;
};

class DatabaseOpenTests: public TestOnDisk {};

TEST_F(DatabaseOpenTests, MaximumPageSize)
{
    // Maximum page size (65,536) is represented as 0 on disk, since it cannot fit into a short integer.
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

TEST_F(BasicDatabaseTests, OpenAndCloseDatabase)
{
    Database db;
    ASSERT_OK(db.open(ROOT, options));
    ASSERT_OK(db.close());
}

TEST_F(BasicDatabaseTests, DestroyDatabase)
{
    Database db;
    ASSERT_OK(db.open(ROOT, options));
    ASSERT_OK(std::move(db).destroy());
}

TEST_F(BasicDatabaseTests, DatabaseIsMovable)
{
    Database db;
    ASSERT_OK(db.open(ROOT, options));
    Database db2 {std::move(db)};
    db = std::move(db2);
    ASSERT_OK(db.close());
}

TEST_F(BasicDatabaseTests, ReopenDatabase)
{
    Database db;
    for (Size i {}; i < 10; ++i) {
        ASSERT_OK(db.open(ROOT, options));
        ASSERT_OK(db.close());
    }
}

static auto insert_random_groups(Database &db, Size num_groups, Size group_size)
{
    RecordGenerator generator;
    Random random {UnitTests::random_seed};

    for (Size iteration {}; iteration < num_groups; ++iteration) {
        const auto records = generator.generate(random, group_size);
        auto itr = cbegin(records);
        ASSERT_OK(db.status());
        auto xact = db.transaction();

        for (Size i {}; i < group_size; ++i) {
            ASSERT_OK(db.insert(itr->key, itr->value));
            itr++;
        }
        ASSERT_OK(xact.commit());
    }
}

static auto traverse_all_records(Database &db)
{
    for (auto c = db.first(); c.is_valid(); ++c) {
        CursorInternal::TEST_validate(c);
    }
    for (auto c = db.last(); c.is_valid(); --c) {
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
        auto xact = db.transaction();

        for (Size i {}; i < GROUP_SIZE; ++i) {
            ASSERT_OK(db.insert(itr->key, itr->value));
            itr++;
        }
        ASSERT_OK(xact.commit());
        ASSERT_OK(db.close());
    }

    ASSERT_OK(db.open(ROOT, options));
    CALICO_EXPECT_EQ(db.statistics().record_count(), records.size());
    for (const auto &[key, value]: records) {
        const auto c = tools::find_exact(db, key);
        ASSERT_TRUE(c.is_valid());
        ASSERT_EQ(c.key().to_string(), key);
        ASSERT_EQ(c.value(), value);
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

        auto xact = db.transaction();
        for (Size i {}; i < NUM_RECORDS; ++i) {
            const auto key = make_key<KEY_WIDTH>(i);
            ASSERT_OK(db.insert(key, key));
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
            for (auto c = db.first(); counter < N; ++c, ++counter) {
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
            auto c = db.find(make_key<KEY_WIDTH>(i));

            for (auto j = i; j < std::min(i + MAX_ROUND_SIZE, NUM_RECORDS); ++j, ++c) {
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
            auto c = db.find(make_key<KEY_WIDTH>(i));
            auto s = c.value();
            strings.emplace_back(std::move(s));
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

} // <anonymous>
