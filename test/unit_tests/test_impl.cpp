
#include "database/database_impl.h"
#include "tools.h"
#include "tree/cursor_internal.h"
#include "tree/header.h"
#include "unit_tests.h"
#include "wal/wal.h"
#include <array>
#include <filesystem>
#include <gtest/gtest.h>
#include <vector>

namespace Calico {

namespace fs = std::filesystem;

class BasicDatabaseTests: public OnDiskTest {
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

    std::string prefix {PREFIX};
    Size frame_count {64};
    Options options;
};

TEST_F(BasicDatabaseTests, OpensAndCloses)
{
    Database *db;
    for (Size i {}; i < 10; ++i) {
        ASSERT_OK(Database::open(ROOT, options, &db));
        delete db;
    }
    ASSERT_TRUE(storage->file_exists(prefix + "data").is_ok());
}

TEST_F(BasicDatabaseTests, IsDestroyed)
{
    std::error_code code;
    fs::remove_all("/tmp/calico_test_wal", code);
    ASSERT_OK(storage->create_directory("/tmp/calico_test_wal"));
    options.wal_prefix = "/tmp/calico_test_wal/wal_file_";

    Database *db;
    ASSERT_OK(Database::open(ROOT, options, &db));
    ASSERT_TRUE(storage->file_exists(prefix + "data").is_ok());
    ASSERT_TRUE(storage->file_exists(options.wal_prefix.to_string() + "1").is_ok());
    delete db;

    // TODO: Ensure that WAL files stored in a separate location are deleted as well.
    ASSERT_OK(Database::destroy(ROOT, options));
    ASSERT_TRUE(storage->file_exists(prefix + "data").is_not_found());
    ASSERT_TRUE(storage->file_exists(options.wal_prefix.to_string() + "1").is_not_found());
}

static auto insert_random_groups(Database &db, Size num_groups, Size group_size)
{
    RecordGenerator generator;
    Tools::RandomGenerator random {4 * 1'024 * 1'024};

    for (Size iteration {}; iteration < num_groups; ++iteration) {
        const auto records = generator.generate(random, group_size);
        auto itr = cbegin(records);
        ASSERT_OK(db.status());

        for (Size i {}; i < group_size; ++i) {
            ASSERT_OK(db.put(itr->key, itr->value));
            itr++;
        }
        ASSERT_OK(db.commit());
    }
}

static auto traverse_all_records(Database &db)
{
    std::unique_ptr<Cursor> c {db.new_cursor()};
    c->seek_first();
    for (; c->is_valid(); c->next()) {
        CursorInternal::TEST_validate(*c);
    }
    c->seek_last();
    for (; c->is_valid(); c->previous()) {
        CursorInternal::TEST_validate(*c);
    }
}

TEST_F(BasicDatabaseTests, InsertOneGroup)
{
    Database *db;
    ASSERT_OK(Database::open(ROOT, options, &db));
    insert_random_groups(*db, 1, 500);
    traverse_all_records(*db);
    delete db;
}

TEST_F(BasicDatabaseTests, InsertMultipleGroups)
{
    Database *db;
    ASSERT_OK(Database::open(ROOT, options, &db));
    insert_random_groups(*db, 10, 500);
    traverse_all_records(*db);
    delete db;
}

TEST_F(BasicDatabaseTests, DataPersists)
{
    static constexpr Size NUM_ITERATIONS {5};
    static constexpr Size GROUP_SIZE {10};

    auto s = ok();
    RecordGenerator generator;
    Tools::RandomGenerator random {4 * 1'024 * 1'024};

    const auto records = generator.generate(random, GROUP_SIZE * NUM_ITERATIONS);
    auto itr = cbegin(records);
    Database *db;

    for (Size iteration {}; iteration < NUM_ITERATIONS; ++iteration) {
        ASSERT_OK(Database::open(ROOT, options, &db));
        ASSERT_OK(db->status());

        for (Size i {}; i < GROUP_SIZE; ++i) {
            ASSERT_OK(db->put(itr->key, itr->value));
            itr++;
        }
        ASSERT_OK(db->commit());
        delete db;
    }

    ASSERT_OK(Database::open(ROOT, options, &db));
    for (const auto &[key, value]: records) {
        std::string value_out;
        ASSERT_OK(TestTools::get(*db, key, value_out));
        ASSERT_EQ(value_out, value);
    }
    delete db;
}

TEST_F(BasicDatabaseTests, ReportsInvalidPageSizes)
{
    auto invalid = options;

    Database *db;
    invalid.page_size = MINIMUM_PAGE_SIZE / 2;
    ASSERT_TRUE(Database::open(ROOT, invalid, &db).is_invalid_argument());

    invalid.page_size = MAXIMUM_PAGE_SIZE * 2;
    ASSERT_TRUE(Database::open(ROOT, invalid, &db).is_invalid_argument());

    Options options;
    invalid.page_size = options.page_size - 1;
    ASSERT_TRUE(Database::open(ROOT, invalid, &db).is_invalid_argument());
}

class ReaderTests: public BasicDatabaseTests {
public:
    static constexpr Size KEY_WIDTH {6};
    static constexpr Size NUM_RECORDS {200};

    auto SetUp() -> void override
    {
        Database *temp;
        ASSERT_OK(Database::open(ROOT, options, &temp));
        db.reset(temp);

        for (Size i {}; i < NUM_RECORDS; ++i) {
            const auto key = Tools::integral_key<KEY_WIDTH>(i);
            ASSERT_OK(db->put(key, key));
        }
        ASSERT_OK(db->commit());
    }

    auto localized_reader() const -> void
    {
        static constexpr Size NUM_ROUNDS {2};

        // Concentrate the cursors on the first N records.
        static constexpr Size N {10};
        static_assert(NUM_RECORDS >= N);

        for (Size i {}; i < NUM_ROUNDS; ++i) {
            Size counter {};
            std::unique_ptr<Cursor> c {db->new_cursor()};
            c->seek_first();
            for (; counter < N; c->next(), ++counter) {
                const auto key = Tools::integral_key<KEY_WIDTH>(counter);
                ASSERT_EQ(c->key().to_string(), key);
                ASSERT_EQ(c->value(), key);
            }
        }
    }

    auto distributed_reader(Size r) const -> void
    {
        static constexpr Size MAX_ROUND_SIZE {10};
        // Try to spread the cursors out across the database.
        const auto first = r * NUM_RECORDS % NUM_RECORDS;
        for (auto i = first; i < NUM_RECORDS; ++i) {
            std::unique_ptr<Cursor> c {db->new_cursor()};
            c->seek(Tools::integral_key<KEY_WIDTH>(i));

            for (auto j = i; j < std::min(i + MAX_ROUND_SIZE, NUM_RECORDS); ++j, c->next()) {
                const auto key = Tools::integral_key<KEY_WIDTH>(j);
                ASSERT_TRUE(c->is_valid());
                ASSERT_EQ(c->key().to_string(), key);
                ASSERT_EQ(c->value(), key);
            }
        }
    }

    Tools::RandomGenerator random {4 * 1'024 * 1'024};
    std::unique_ptr<Database> db;
};

TEST_F(ReaderTests, SingleReader)
{
    for (Size x {}; x < 1'000; ++x) {
        std::vector<std::string> strings;
        for (Size i {}; i < NUM_RECORDS; ++i) {
            std::unique_ptr<Cursor> c {db->new_cursor()};
            c->seek(Tools::integral_key<KEY_WIDTH>(i));
            ASSERT_TRUE(c->is_valid());
            strings.emplace_back(c->value().to_string());
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
        return TestTools::snapshot(*options.storage, options.page_size);
    }

    Options options;
    Tools::RandomGenerator random {4 * 1'024 * 1'024};
    std::vector<Record> records;
    std::unique_ptr<DatabaseImpl> impl;
};

class DbAbortTests: public InMemoryTest {
protected:
    DbAbortTests()
    {
        db = std::make_unique<TestDatabase>(*storage);
    }
    ~DbAbortTests() override = default;

    std::unique_ptr<TestDatabase> db;
};

static auto add_records(TestDatabase &test, Size n, Size max_value_size, const std::string &prefix = {})
{
    std::vector<Record> records(n);

    for (Size i {}; i < n; ++i) {
        const auto value_size = test.random.Next<Size>(max_value_size);
        records[i].key = prefix + Tools::integral_key(i);
        records[i].value = test.random.Generate(value_size).to_string();
        EXPECT_OK(test.impl->put(records[i].key, records[i].value));
    }
    return records;
}

TEST_F(DbAbortTests, RevertsFirstBatch)
{
    const auto snapshot = db->snapshot();
    add_records(*db, 3, 0x400);
    ASSERT_OK(db->impl->abort());
    ASSERT_EQ(snapshot, db->snapshot());
}

TEST_F(DbAbortTests, RevertsSecondBatch)
{
    add_records(*db, 3, 0x400, "_");
    ASSERT_OK(db->impl->commit());

    // Hack to make sure the database file is up-to-date.
    (void)db->impl->pager->flush({});

    const auto snapshot = db->snapshot();
    add_records(*db, 3, 0x400);
    ASSERT_OK(db->impl->abort());
    ASSERT_EQ(snapshot, db->snapshot());
}

TEST_F(DbAbortTests, RevertsNthBatch)
{
    for (Size i {}; i < 10; ++i) {
        add_records(*db, 100, 0x400, "_");
        ASSERT_OK(db->impl->commit());
    }
    // Hack to make sure the database file is up-to-date.
    (void)db->impl->pager->flush({});

    const auto snapshot = db->snapshot();
    add_records(*db, 1'000, 0x400);
    ASSERT_OK(db->impl->abort());
    ASSERT_EQ(snapshot, db->snapshot());
}

class DbRecoveryTests: public InMemoryTest {
protected:
    ~DbRecoveryTests() override = default;
};

TEST_F(DbRecoveryTests, RecoversFirstBatch)
{
    std::unique_ptr<Storage> clone;
    std::string snapshot;

    {
        TestDatabase db {*storage};
        add_records(db, 100, 10 * db.options.page_size);
        ASSERT_OK(db.impl->commit());

        // Simulate a crash by cloning the database before cleanup has occurred.
        clone.reset(dynamic_cast<const Tools::DynamicMemory &>(*storage).clone());

        (void)db.impl->pager->flush({});
        snapshot = db.snapshot();
    }
    // Create a new database from the cloned data. This database will need to roll the WAL forward to become
    // consistent.
    TestDatabase clone_db {*clone};
    ASSERT_OK(clone_db.impl->status());
    auto s = clone_db.snapshot();
    ASSERT_EQ(snapshot, s);
}

TEST_F(DbRecoveryTests, RecoversNthBatch)
{
    std::unique_ptr<Storage> clone;
    std::string snapshot;

    {
        TestDatabase db {*storage};

        for (Size i {}; i < 10; ++i) {
            add_records(db, 100, 10 * db.options.page_size);
            ASSERT_OK(db.impl->commit());
        }

        clone.reset(dynamic_cast<const Tools::DynamicMemory &>(*storage).clone());

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

class DbErrorTests: public ParameterizedInMemoryTest<Size> {
protected:
    DbErrorTests()
    {
        storage = std::make_unique<Tools::DynamicMemory>();
        EXPECT_OK(storage->create_directory("test"));
        db = std::make_unique<TestDatabase>(*storage);

        committed = add_records(*db, 5'000, 10);
        EXPECT_OK(db->impl->commit());

        storage_handle().add_interceptor(
            Tools::Interceptor {
                "test/data",
                Tools::Interceptor::READ,
                [this] {
                    if (counter++ >= GetParam()) {
                        return special_error();
                    }
                    return ok();
                }});
    }
    ~DbErrorTests() override = default;

    std::unique_ptr<TestDatabase> db;
    std::vector<Record> committed;
    Size counter {};
};

TEST_P(DbErrorTests, HandlesReadErrorDuringQuery)
{
    for (Size iteration {}; iteration < 2; ++iteration) {
        for (Size i {}; i < committed.size(); ++i) {
            std::string value;
            const auto s = db->impl->get(Tools::integral_key(i), value);

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
    std::unique_ptr<Cursor> cursor {db->impl->new_cursor()};
    cursor->seek_first();
    while (cursor->is_valid()) {
        (void)cursor->key();
        (void)cursor->value();
        cursor->next();
    }
    assert_special_error(cursor->status());
    ASSERT_OK(db->impl->status());
    counter = 0;

    cursor->seek_last();
    while (cursor->is_valid()) {
        (void)cursor->key();
        (void)cursor->value();
        cursor->previous();
    }
    assert_special_error(cursor->status());
    ASSERT_OK(db->impl->status());
}

INSTANTIATE_TEST_SUITE_P(
    DbErrorTests,
    DbErrorTests,
    ::testing::Values(
        0,
        1,
        10,
        100));

struct ErrorWrapper {
    ErrorTarget target {};
    Size successes {};
};

class DbFatalErrorTests: public ParameterizedInMemoryTest<ErrorWrapper> {
protected:
    DbFatalErrorTests()
    {
        db = std::make_unique<TestDatabase>(*storage);

        committed = add_records(*db, 5'000, 10);
        EXPECT_OK(db->impl->commit());

        const auto make_interceptor = [this](const auto &prefix, auto type) {
            return Tools::Interceptor {prefix, type, [this] {
                if (counter++ >= GetParam().successes) {
                    return special_error();
                }
                return ok();
            }};
        };

        switch (GetParam().target) {
            case ErrorTarget::DATA_READ:
                storage_handle().add_interceptor(make_interceptor("test/data", Tools::Interceptor::READ));
                break;
            case ErrorTarget::DATA_WRITE:
                storage_handle().add_interceptor(make_interceptor("test/data", Tools::Interceptor::WRITE));
                break;
            case ErrorTarget::WAL_READ:
                storage_handle().add_interceptor(make_interceptor("test/wal", Tools::Interceptor::READ));
                break;
            case ErrorTarget::WAL_WRITE:
                storage_handle().add_interceptor(make_interceptor("test/wal", Tools::Interceptor::WRITE));
                break;
        }
    }

    ~DbFatalErrorTests() override = default;

    std::unique_ptr<TestDatabase> db;
    std::vector<Record> committed;
    Size counter {};
};

TEST_P(DbFatalErrorTests, ErrorsDuringModificationsAreFatal)
{
    while (db->impl->status().is_ok()) {
        for (Size i {}; i < committed.size() && db->impl->erase(Tools::integral_key(i)).is_ok(); ++i);
        for (Size i {}; i < committed.size() && db->impl->put(Tools::integral_key(i), "value").is_ok(); ++i);
    }
    assert_special_error(db->impl->status());
    assert_special_error(db->impl->put("key", "value"));
}

TEST_P(DbFatalErrorTests, RecoversFromFatalErrors)
{
    Size i {};
    while (i < committed.size() && db->impl->erase(Tools::integral_key(i++)).is_ok());
    assert_special_error(db->impl->status());
    assert_special_error(db->impl->commit());
    assert_special_error(db->impl->put("key", "value"));
    assert_special_error(db->impl->close());

    storage_handle().clear_interceptors();
    db->impl = std::make_unique<DatabaseImpl>();
    ASSERT_OK(db->impl->open("test", db->options));

    for (const auto &[key, value]: committed) {
        TestTools::expect_contains(*db->impl, key, value);
    }
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

class ExtendedCursor : public Cursor {
    friend class ExtendedDatabase;

    explicit ExtendedCursor(Cursor *base)
        : m_base {base}
    {}

    std::unique_ptr<Cursor> m_base;

public:
    ~ExtendedCursor() override = default;

    [[nodiscard]]
    auto is_valid() const -> bool override
    {
        return m_base->is_valid();
    }

    [[nodiscard]]
    auto status() const -> Status override
    {
        return m_base->status();
    }

    [[nodiscard]]
    auto key() const -> Slice override
    {
        return m_base->key();
    }

    [[nodiscard]]
    auto value() const -> Slice override
    {
        return m_base->value();
    }

    auto seek(const Slice &key) -> void override
    {
        return m_base->seek(key);
    }

    auto seek_first() -> void override
    {
        return m_base->seek_first();
    }

    auto seek_last() -> void override
    {
        return m_base->seek_last();
    }

    auto next() -> void override
    {
        return m_base->next();
    }

    auto previous() -> void override
    {
        return m_base->previous();
    }
};

class ExtendedDatabase : public Database {
    std::unique_ptr<Tools::DynamicMemory> m_storage;
    std::unique_ptr<Database> m_base;

public:
    [[nodiscard]]
    static auto open(const Slice &base, Options options, ExtendedDatabase **out) -> Status
    {
        const auto prefix = base.to_string() + "_";
        auto *ext = new(std::nothrow) ExtendedDatabase;
        if (ext == nullptr) {
            return system_error("cannot allocate extension database: out of memory");
        }
        auto storage = std::make_unique<Tools::DynamicMemory>();
        options.storage = storage.get();

        Database *db;
        Calico_Try_S(Database::open(base, options, &db));

        ext->m_base.reset(db);
        ext->m_storage = std::move(storage);
        *out = ext;
        return Status::ok();
    }

    ~ExtendedDatabase() override = default;

    [[nodiscard]]
    auto get_property(const Slice &name) const -> std::string override
    {
        return m_base->get_property(name);
    }

    [[nodiscard]]
    auto new_cursor() const -> Cursor * override
    {
        return new ExtendedCursor {m_base->new_cursor()};
    }

    [[nodiscard]]
    auto status() const -> Status override
    {
        return m_base->status();
    }

    [[nodiscard]]
    auto vacuum() -> Status override
    {
        return m_base->vacuum();
    }

    [[nodiscard]]
    auto commit() -> Status override
    {
        return m_base->commit();
    }

    [[nodiscard]]
    auto abort() -> Status override
    {
        return m_base->abort();
    }

    [[nodiscard]]
    auto get(const Slice &key, std::string &value) const -> Status override
    {
        return m_base->get(key, value);
    }

    [[nodiscard]]
    auto put(const Slice &key, const Slice &value) -> Status override
    {
        return m_base->put(key, value);
    }

    [[nodiscard]]
    auto erase(const Slice &key) -> Status override
    {
        return m_base->erase(key);
    }
};

TEST(ExtensionTests, Extensions)
{
    ExtendedDatabase *db;
    ASSERT_OK(ExtendedDatabase::open("test", {}, &db));
    ASSERT_OK(db->put("a", "1"));
    ASSERT_OK(db->put("b", "2"));
    ASSERT_OK(db->put("c", "3"));

    auto *cursor = db->new_cursor();
    cursor->seek_first();
    ASSERT_TRUE(cursor->is_valid());
    ASSERT_EQ(cursor->key(), "a");
    ASSERT_EQ(cursor->value(), "1");
    cursor->next();
    ASSERT_TRUE(cursor->is_valid());
    ASSERT_EQ(cursor->key(), "b");
    ASSERT_EQ(cursor->value(), "2");
    cursor->next();
    ASSERT_TRUE(cursor->is_valid());
    ASSERT_EQ(cursor->key(), "c");
    ASSERT_EQ(cursor->value(), "3");
    cursor->next();
    ASSERT_FALSE(cursor->is_valid());
    delete cursor;

    ASSERT_OK(db->commit());
    delete db;
}

class ApiTests: public InMemoryTest {
protected:
    ApiTests()
    {
        options.storage = storage.get();
    }

    ~ApiTests() override
    {
        delete db;
    }

    auto SetUp() -> void override
    {
        ASSERT_OK(Calico::Database::open(ROOT, options, &db));
    }

    Options options;
    Database *db;
};

TEST_F(ApiTests, IsConstCorrect)
{
    ASSERT_OK(db->put("key", "value"));

    std::string value;
    const auto *const_db = db;
    ASSERT_OK(const_db->get("key", value));
    ASSERT_EQ(const_db->get_property("calico.count.records"), "1");
    ASSERT_OK(const_db->status());

    auto *cursor = const_db->new_cursor();
    cursor->seek_first();

    const auto *const_cursor = cursor;
    ASSERT_TRUE(const_cursor->is_valid());
    ASSERT_OK(const_cursor->status());
    ASSERT_EQ(const_cursor->key(), "key");
    ASSERT_EQ(const_cursor->value(), "value");
    delete const_cursor;
}

TEST_F(ApiTests, UncommittedTransactionIsRolledBack)
{
    ASSERT_OK(db->put("a", "1"));
    ASSERT_OK(db->put("b", "2"));
    ASSERT_OK(db->put("c", "3"));
    ASSERT_OK(db->commit());

    ASSERT_OK(db->put("a", "x"));
    ASSERT_OK(db->put("b", "y"));
    ASSERT_OK(db->put("c", "z"));
    delete db;

    ASSERT_OK(Calico::Database::open(ROOT, options, &db));
    auto *cursor = db->new_cursor();
    cursor->seek_first();
    ASSERT_TRUE(cursor->is_valid());
    ASSERT_EQ(cursor->key(), "a");
    ASSERT_EQ(cursor->value(), "1");

    cursor->next();
    ASSERT_TRUE(cursor->is_valid());
    ASSERT_EQ(cursor->key(), "b");
    ASSERT_EQ(cursor->value(), "2");

    cursor->next();
    ASSERT_TRUE(cursor->is_valid());
    ASSERT_EQ(cursor->key(), "c");
    ASSERT_EQ(cursor->value(), "3");

    cursor->next();
    ASSERT_FALSE(cursor->is_valid());
    delete cursor;
}

TEST_F(ApiTests, EmptyTransactionsAreOk)
{
    ASSERT_OK(db->commit());
    ASSERT_OK(db->abort());
}

TEST_F(ApiTests, KeysCanBeArbitraryBytes)
{
    const std::string key_1 {"\x00\x00", 2};
    const std::string key_2 {"\x00\x01", 2};
    const std::string key_3 {"\x01\x00", 2};

    ASSERT_OK(db->put(key_1, "1"));
    ASSERT_OK(db->put(key_2, "2"));
    ASSERT_OK(db->put(key_3, "3"));
    ASSERT_OK(db->commit());

    auto *cursor = db->new_cursor();
    cursor->seek_first();

    ASSERT_OK(cursor->status());
    ASSERT_EQ(cursor->key(), key_1);
    ASSERT_EQ(cursor->value(), "1");
    cursor->next();

    ASSERT_OK(cursor->status());
    ASSERT_EQ(cursor->key(), key_2);
    ASSERT_EQ(cursor->value(), "2");
    cursor->next();

    ASSERT_OK(cursor->status());
    ASSERT_EQ(cursor->key(), key_3);
    ASSERT_EQ(cursor->value(), "3");
    cursor->next();
    delete cursor;
}

class CommitTests : public ApiTests {
protected:
    ~CommitTests() override = default;

    auto SetUp() -> void override
    {
        ApiTests::SetUp();
        ASSERT_OK(db->put("a", "1"));
        ASSERT_OK(db->put("b", "2"));
        ASSERT_OK(db->put("c", "3"));
    }

    auto TearDown() -> void override
    {
        ASSERT_OK(db->commit());
        assert_special_error(db->put("d", "4"));
        delete db;

        storage_handle().clear_interceptors();
        ASSERT_OK(Database::open("test", options, &db));

        std::string value;
        ASSERT_OK(db->get("a", value));
        ASSERT_EQ(value, "1");
        ASSERT_OK(db->get("b", value));
        ASSERT_EQ(value, "2");
        ASSERT_OK(db->get("c", value));
        ASSERT_EQ(value, "3");
    }
};

TEST_F(CommitTests, WalAdvanceFailure)
{
    // Write the commit record and flush successfully, but fail to open the next segment file.
    Quick_Interceptor("test/wal", Tools::Interceptor::OPEN);
}

TEST_F(CommitTests, PagerFlushFailure)
{
    // Write the commit record and flush successfully, but fail to flush old pages from the page cache.
    Quick_Interceptor("test/data", Tools::Interceptor::WRITE);
}

class WalPrefixTests : public OnDiskTest {
public:
    WalPrefixTests()
    {
        options.storage = storage.get();
    }

    Options options;
    Database *db {};
};

TEST_F(WalPrefixTests, WalDirectoryMustExist)
{
    options.wal_prefix = "nonexistent";
    ASSERT_TRUE(Calico::Database::open(ROOT, options, &db).is_not_found());
}

} // <anonymous>
