
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
        options.cache_size = options.page_size * frame_count;
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
    dynamic_cast<const DatabaseImpl &>(db).TEST_validate(); //TODO: The tree validation method sucks and fails when the tree is too big. We end up running out of frames.
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
    insert_random_groups(*db, 1, 2734);
    traverse_all_records(*db);
    delete db;
}

TEST_F(BasicDatabaseTests, DataPersists)
{
    static constexpr Size NUM_ITERATIONS {5};
    static constexpr Size GROUP_SIZE {10};

    auto s = Status::ok();
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

TEST_F(BasicDatabaseTests, TwoDatabases)
{
    Database *lhs;
    expect_ok(Database::open("/tmp/calico_test_1", options, &lhs));

    Database *rhs;
    expect_ok(Database::open("/tmp/calico_test_2", options, &rhs));

    for (Size i {}; i < 10; ++i) {
        expect_ok(lhs->put(Tools::integral_key(i), "value"));
    }
    expect_ok(lhs->commit());

    auto *cursor = lhs->new_cursor();
    cursor->seek_first();
    while (cursor->is_valid()) {
        const auto k = cursor->key();
        const auto v = cursor->value();
        expect_ok(rhs->put(k, v));
        cursor->next();
    }
    delete cursor;

    expect_ok(rhs->commit());

    Size i {};
    cursor = rhs->new_cursor();
    cursor->seek_first();
    while (cursor->is_valid()) {
        const auto k = cursor->key();
        const auto v = cursor->value();
        ASSERT_EQ(k, Tools::integral_key(i++));
        ASSERT_EQ(v, "value");
        cursor->next();
    }
    delete cursor;

    delete lhs;
    delete rhs;

    expect_ok(Database::destroy("/tmp/calico_test_1", options));
    expect_ok(Database::destroy("/tmp/calico_test_2", options));
}

class DbVacuumTests: public ParameterizedOnDiskTest<std::tuple<Size, Size, bool>>
{
public:
    DbVacuumTests()
        : lower_bounds {std::get<0>(GetParam())},
          upper_bounds {std::get<1>(GetParam())},
          reopen {std::get<2>(GetParam())}
    {
        options.page_size = 0x200;
        options.cache_size = 0x200 * 16;
    }

    std::unordered_map<std::string, std::string> map;
    Tools::RandomGenerator random {1'024 * 1'024 * 8};
    Database *db;
    Options options;
    Size lower_bounds {};
    Size upper_bounds {};
    bool reopen {};
};

TEST_P(DbVacuumTests, SanityCheck)
{
    ASSERT_OK(Database::open(ROOT, options, &db));

    for (Size iteration {}; iteration < 4; ++iteration) {
        if (reopen) {
            delete db;
            ASSERT_OK(Database::open(ROOT, options, &db));
        }
        while (map.size() < upper_bounds) {
            const auto key = random.Generate(10);
            const auto value = random.Generate(options.page_size * 2);
            ASSERT_OK(db->put(key, value));
            map[key.to_string()] = value.to_string();
        }
        while (map.size() > lower_bounds) {
            const auto key = begin(map)->first;
            map.erase(key);
            ASSERT_OK(db->erase(key));
        }
        ASSERT_OK(db->commit());
        ASSERT_OK(db->vacuum());
        dynamic_cast<DatabaseImpl &>(*db).TEST_validate();

        Size i {};
        for (const auto &[key, value]: map) {
            i++;
            std::string result;
            ASSERT_OK(db->get(key, result));
            CALICO_EXPECT_EQ(result, value);
        }
    }
    delete db;
}

INSTANTIATE_TEST_SUITE_P(
    DbVacuumTests,
    DbVacuumTests,
    ::testing::Values(
        std::make_tuple(1, 2, false),
        std::make_tuple(1, 2, true),
        std::make_tuple(10, 20, false),
        std::make_tuple(10, 20, true),
        std::make_tuple(100, 200, false),
        std::make_tuple(100, 200, true),
        std::make_tuple(90, 110, false),
        std::make_tuple(90, 110, true)));

class TestDatabase {
public:
    explicit TestDatabase(Storage &storage)
    {
        options.page_size = 0x200;
        options.cache_size = 32 * options.page_size;
        options.storage = &storage;

        const auto s = reopen();
        EXPECT_TRUE(s.is_ok()) << "Error: " << s.what().to_string();
    }

    ~TestDatabase() = default;

    [[nodiscard]]
    auto reopen() -> Status
    {
        impl = std::make_unique<DatabaseImpl>();
        return impl->open("test", options);
    }

    auto snapshot() const -> std::string
    {
        return TestTools::snapshot(*options.storage, options.page_size);
    }

    Options options;
    Tools::RandomGenerator random {4 * 1'024 * 1'024};
    std::vector<Record> records;
    std::unique_ptr<DatabaseImpl> impl;
};

class DbRevertTests: public InMemoryTest {
protected:
    DbRevertTests()
    {
        db = std::make_unique<TestDatabase>(*storage);
    }
    ~DbRevertTests() override = default;

    std::unique_ptr<TestDatabase> db;
};

static auto add_records(TestDatabase &test, Size n, Size max_value_size, const std::string &prefix = {})
{
    std::map<std::string, std::string> records;

    for (Size i {}; i < n; ++i) {
        const auto value_size = test.random.Next<Size>(max_value_size);
        const auto key = prefix + Tools::integral_key(i);
        const auto value = test.random.Generate(value_size).to_string();
        EXPECT_OK(test.impl->put(key, value));
        records[key] = value;
    }
    return records;
}

TEST_F(DbRevertTests, RevertsUncommittedBatch)
{
    add_records(*db, 100, 0x400, "_");
    ASSERT_OK(db->impl->commit());

    // Hack to make sure the database file is up-to-date.
    (void)db->impl->pager->flush({});

    const auto snapshot = db->snapshot();
    add_records(*db, 1'000, 0x400);
    ASSERT_OK(db->reopen());
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
                    return Status::ok();
                }});
    }
    ~DbErrorTests() override = default;

    std::unique_ptr<TestDatabase> db;
    std::map<std::string, std::string> committed;
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

        // Make sure all page types are represented in the database.
        committed = add_records(*db, 5'000, db->options.page_size * 2);
        for (Size i {}; i < 500; ++i) {
            const auto itr = begin(committed);
            EXPECT_OK(db->impl->erase(itr->first));
            committed.erase(itr);
        }

        EXPECT_OK(db->impl->commit());

        const auto make_interceptor = [this](const auto &prefix, auto type) {
            return Tools::Interceptor {prefix, type, [this] {
                if (counter++ >= GetParam().successes) {
                    return special_error();
                }
                return Status::ok();
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
    std::map<std::string, std::string> committed;
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

TEST_P(DbFatalErrorTests, OperationsAreNotPermittedAfterFatalError)
{
    auto itr = begin(committed);
    while (db->impl->erase(itr++->first).is_ok()) {
        ASSERT_NE(itr, end(committed));
    }
    assert_special_error(db->impl->status());
    assert_special_error(db->impl->commit());
    assert_special_error(db->impl->put("key", "value"));
    std::string value;
    assert_special_error(db->impl->get("key", value));
    auto *cursor = db->impl->new_cursor();
    assert_special_error(cursor->status());
    delete cursor;
}

TEST_P(DbFatalErrorTests, RecoversFromFatalErrors)
{
    auto itr = begin(committed);
    for (; ; ) {
        auto s = db->impl->erase(itr++->first);
        if (!s.is_ok()) {
            assert_special_error(s);
            break;
        }
        ASSERT_NE(itr, end(committed));
    }
    db->impl.reset();

    storage_handle().clear_interceptors();
    db->impl = std::make_unique<DatabaseImpl>();
    ASSERT_OK(db->impl->open("test", db->options));

    for (const auto &[key, value]: committed) {
        TestTools::expect_contains(*db->impl, key, value);
    }
    Tools::validate_db(*db->impl);
}

TEST_P(DbFatalErrorTests, VacuumReportsErrors)
{
    assert_special_error(db->impl->vacuum());
    assert_special_error(db->impl->status());
}

// TODO: This doesn't exercise much of what can go wrong here. Need a test for failure to truncate the file, so the
//       header page count is left incorrect. We should be able to recover from that.
TEST_P(DbFatalErrorTests, RecoversFromVacuumFailure)
{
    assert_special_error(db->impl->vacuum());
    db->impl.reset();

    storage_handle().clear_interceptors();
    db->impl = std::make_unique<DatabaseImpl>();
    ASSERT_OK(db->impl->open("test", db->options));

    for (const auto &[key, value]: committed) {
        TestTools::expect_contains(*db->impl, key, value);
    }
    Tools::validate_db(*db->impl);

    Size file_size;
    ASSERT_OK(storage->file_size("test/data", file_size));
    std::string property;
    ASSERT_TRUE(db->impl->get_property("calico.counts", property));
    const auto counts = Tools::parse_db_counts(property);
    ASSERT_EQ(file_size, counts.pages * db->options.page_size);
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
            return Status::system_error("cannot allocate extension database: out of memory");
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
    auto get_property(const Slice &name, std::string &out) const -> bool override
    {
        return m_base->get_property(name, out);
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
        return Status::ok();
    }

    [[nodiscard]]
    auto commit() -> Status override
    {
        return m_base->commit();
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
    std::string property;
    ASSERT_TRUE(const_db->get_property("calico.counts", property));
    ASSERT_EQ(property, "records:1,pages:1,updates:1");
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

class CommitFailureTests : public ApiTests {
protected:
    ~CommitFailureTests() override = default;

    auto SetUp() -> void override
    {
        ApiTests::SetUp();
        ASSERT_OK(db->put("a", "1"));
        ASSERT_OK(db->put("b", "2"));
        ASSERT_OK(db->put("c", "3"));
    }

    auto TearDown() -> void override
    {
        assert_special_error(db->commit());
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

TEST_F(CommitFailureTests, WalAdvanceFailure)
{
    // Write the commit record and flush successfully, but fail to open the next segment file.
    Quick_Interceptor("test/wal", Tools::Interceptor::OPEN);
}

TEST_F(CommitFailureTests, PagerFlushFailure)
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
