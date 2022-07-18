
#include <array>
#include <filesystem>
#include <vector>
#include <unordered_set>

#include <gtest/gtest.h>

#include "db/database_impl.h"
#include "pool/interface.h"
#include "storage/file.h"
#include "storage/system.h"

#include "tree/tree.h"
#include "fakes.h"

#include "tools.h"
#include "utils/logging.h"

namespace {

using namespace cco;
namespace fs = std::filesystem;
constexpr auto BASE = "/tmp/__calico_database_tests";

class TestDatabase {
public:
    TestDatabase()
    {
        std::unique_ptr<IDirectory> home;
        Database::Impl::Parameters param;
        param.options.page_size = 0x200;
        param.options.frame_count = 16;

        home = std::make_unique<FakeDirectory>("");
        impl = Database::Impl::open(param, std::move(home)).value();
        fake = dynamic_cast<FakeDirectory*>(&impl->home());
        data_controls = fake->get_faults("data");
        wal_controls = fake->get_faults("wal");

        RecordGenerator::Parameters generator_param;
        generator_param.mean_key_size = 20;
        generator_param.mean_value_size = 50;
        generator_param.spread = 15;
        auto generator = RecordGenerator {generator_param};

        records = generator.generate(random, 1'500);
        for (const auto &[key, value]: records)
            tools::insert(*impl, key, value);
        std::sort(begin(records), end(records));
    }

    ~TestDatabase()
    {
        data_controls.set_read_fault_rate(0);
        wal_controls.set_read_fault_rate(0);
        data_controls.set_read_fault_counter(-1);
        wal_controls.set_read_fault_counter(-1);

        data_controls.set_write_fault_rate(0);
        wal_controls.set_write_fault_rate(0);
        data_controls.set_write_fault_counter(-1);
        wal_controls.set_write_fault_counter(-1);
    }

    Random random {0};
    FakeDirectory *fake {};
    FaultControls data_controls {};
    FaultControls wal_controls {};
    std::vector<Record> records;
    std::unique_ptr<Database::Impl> impl;
};

class DatabaseReadFaultTests: public testing::Test {
public:
    DatabaseReadFaultTests() = default;
    ~DatabaseReadFaultTests() override = default;

    TestDatabase db;
};

TEST_F(DatabaseReadFaultTests, SystemErrorIsStoredInCursor)
{
    auto cursor = db.impl->find_minimum();
    ASSERT_TRUE(cursor.is_valid());
    db.data_controls.set_read_fault_rate(100);
    while (cursor.increment()) {}
    ASSERT_FALSE(cursor.is_valid());
    ASSERT_TRUE(cursor.status().is_system_error());
}

TEST_F(DatabaseReadFaultTests, StateIsUnaffectedByReadFaults)
{
    static constexpr auto STEP = 10;
    unsigned r {}, num_faults {};
    for (; r <= 100; r += STEP) {
        db.data_controls.set_read_fault_rate(100 - r);
        auto cursor = db.impl->find_minimum();
        while (cursor.increment()) {}
        ASSERT_FALSE(cursor.is_valid());
        num_faults += !cursor.status().is_ok();
    }
    ASSERT_GT(num_faults, 0);

    db.data_controls.set_read_fault_rate(0);
    for (const auto &[key, value]: db.records) {
        auto cursor = tools::find(*db.impl, key);
        ASSERT_TRUE(cursor.is_valid());
        ASSERT_EQ(cursor.value(), value);
    }
}

class DatabaseWriteFaultTests: public testing::Test {
public:
    DatabaseWriteFaultTests()
    {
        EXPECT_TRUE(db.impl->commit());

        // Mess up the database.
        auto generator = RecordGenerator {{}};
        for (const auto &[key, value]: generator.generate(db.random, 2'500)) {
            if (const auto r = db.random.next_int(8); r == 0) {
                EXPECT_TRUE(db.impl->erase(db.impl->find_minimum()));
            } else if (r == 1) {
                EXPECT_TRUE(db.impl->erase(db.impl->find_maximum()));
            }
            EXPECT_TRUE(tools::insert(*db.impl, key, value));
        }
    }
    ~DatabaseWriteFaultTests() override = default;

    std::vector<Record> uncommitted;
    TestDatabase db;
};

TEST_F(DatabaseWriteFaultTests, InvalidArgumentErrorsDoNotCauseLockup)
{
    const auto empty_key_result = db.impl->insert(stob(""), stob("value"));
    ASSERT_FALSE(empty_key_result.has_value());
    ASSERT_TRUE(empty_key_result.error().is_invalid_argument());
    ASSERT_TRUE(db.impl->insert(stob("*"), stob("value")));

    const std::string long_key(db.impl->info().maximum_key_size() + 1, 'x');
    const auto long_key_result = db.impl->insert(stob(long_key), stob("value"));
    ASSERT_FALSE(long_key_result.has_value());
    ASSERT_TRUE(long_key_result.error().is_invalid_argument());
    ASSERT_TRUE(db.impl->insert(stob(long_key).truncate(long_key.size() - 1), stob("value")));
}

template<class RateSetter>
auto abort_until_successful(TestDatabase &db, RateSetter &&setter)
{
    for (unsigned rate {100}; rate >= 50; rate -= 10) {
        setter(rate);
        ASSERT_TRUE(db.impl->abort().error().is_system_error());
    }
    setter(0);
    auto x = db.impl->abort();
     ASSERT_TRUE(x);
}

auto validate_after_abort(TestDatabase &db)
{
    // db.records contains the set of records in the database after the first commit. The constructor for the "write fault tests"
    // adds some records and deletes others, so if abort() didn't do its job, the database will contain different records. Removing
    // all the records here makes sure the tree connections are still valid.
    for (const auto &[key, value]: db.records) {
        auto cursor = tools::find(*db.impl, key);
        ASSERT_TRUE(cursor.is_valid());
        ASSERT_EQ(cursor.value(), value);
        ASSERT_TRUE(db.impl->erase(cursor));
    }
    ASSERT_EQ(db.impl->info().record_count(), 0);
}

TEST_F(DatabaseWriteFaultTests, AbortIsReentrantAfterDataWriteFaults)
{
    abort_until_successful(db, [this](unsigned rate) {
        db.data_controls.set_write_fault_rate(rate);
    });
    validate_after_abort(db);
}

TEST_F(DatabaseWriteFaultTests, AbortIsReentrantAfterDataReadFaults)
{
    abort_until_successful(db, [this](unsigned rate) {
        db.data_controls.set_read_fault_rate(rate);
    });
    validate_after_abort(db);
}

TEST_F(DatabaseWriteFaultTests, AbortIsReentrantAfterWALReadFaults)
{
    abort_until_successful(db, [this](unsigned rate) {
        db.wal_controls.set_read_fault_rate(rate);
    });
    validate_after_abort(db);
}

TEST_F(DatabaseWriteFaultTests, AbortFixesLockup)
{
    db.data_controls.set_write_fault_rate(100);
    for (Index i {}; ; ++i) {
        const auto s = std::to_string(i);
        auto result = db.impl->insert(stob(s), stob(s));
        if (!result.has_value()) {
            ASSERT_TRUE(result.error().is_system_error());
            // None of the following operations should succeed until an abort() call is successful.
            ASSERT_TRUE(db.impl->insert(stob(s), stob(s)).error().is_system_error());
            ASSERT_TRUE(db.impl->erase(stob(s)).error().is_system_error());
            ASSERT_TRUE(db.impl->find(stob(s)).status().is_system_error());
            ASSERT_TRUE(db.impl->find_minimum().status().is_system_error());
            ASSERT_TRUE(db.impl->find_maximum().status().is_system_error());
            ASSERT_TRUE(db.impl->commit().error().is_system_error());
            break;
        }
    }
    // Might as well let it fail a few times. abort() should be reentrant anyway.
    while (!db.impl->abort().has_value()) {
        const auto rate = db.data_controls.write_fault_rate();
        db.data_controls.set_write_fault_rate(2 * rate / 3);
    }

    validate_after_abort(db);
}

class DatabaseTests: public testing::Test {
public:
    DatabaseTests()
    {
        options.path = BASE;
        options.page_size = 0x200;
        options.frame_count = 16;

        RecordGenerator::Parameters param;
        param.mean_key_size = 20;
        param.mean_value_size = 20;
        param.spread = 15;
        generator = RecordGenerator {param};

        // Make sure the database does not exist already.
        std::error_code ignore;
        fs::remove_all(BASE, ignore);
    }

    Random random {0};
    Options options;
    RecordGenerator generator;
};

TEST_F(DatabaseTests, DataPersists)
{
    static constexpr Size NUM_ITERATIONS {10};
    static constexpr Size GROUP_SIZE {500};

    const auto records = generator.generate(random, GROUP_SIZE * NUM_ITERATIONS);
    auto itr = std::cbegin(records);

    for (Index iteration {}; iteration < NUM_ITERATIONS; ++iteration) {
        Database db {options};
        ASSERT_TRUE(db.open().is_ok());

        for (Index i {}; i < GROUP_SIZE; ++i) {
            ASSERT_TRUE(db.insert(*itr).is_ok());
            itr++;
        }
        ASSERT_TRUE(db.close().is_ok());
    }

    Database db {options};
    ASSERT_TRUE(db.open().is_ok());
    CCO_EXPECT_EQ(db.info().record_count(), records.size());
    for (const auto &[key, value]: records) {
        const auto c = tools::find_exact(db, key);
        ASSERT_TRUE(c.is_valid());
        ASSERT_EQ(btos(c.key()), key);
        ASSERT_EQ(c.value(), value);
    }
    ASSERT_TRUE(db.close().is_ok());
}

TEST_F(DatabaseTests, SanityCheck)
{
    static constexpr Size NUM_ITERATIONS {5};
    static constexpr Size GROUP_SIZE {1'000};
    Options options;
    options.path = BASE;
    options.page_size = 0x200;
    options.frame_count = 16;

    RecordGenerator::Parameters param;
    param.mean_key_size = 20;
    param.mean_value_size = 20;
    param.spread = 15;
    RecordGenerator generator {param};
    Random random {0};

    // Make sure the database does not exist already.
    std::error_code ignore;
    fs::remove_all(BASE, ignore);

    for (Index iteration {}; iteration < NUM_ITERATIONS; ++iteration) {
        Database db {options};
        ASSERT_TRUE(db.open().is_ok());

        for (const auto &record: generator.generate(random, GROUP_SIZE))
            db.insert(record);
        ASSERT_TRUE(db.close().is_ok());
    }

    for (Index iteration {}; iteration < NUM_ITERATIONS; ++iteration) {
        Database db {options};
        ASSERT_TRUE(db.open().is_ok());

        for (const auto &record: generator.generate(random, GROUP_SIZE)) {
            auto r = db.erase(record.key);
            if (r.is_not_found())
                r = db.erase(db.find_minimum());

            if (!r.is_ok())
                ADD_FAILURE() << "cannot find record to remove";
        }
        ASSERT_TRUE(db.commit().is_ok());
        ASSERT_TRUE(db.close().is_ok());
    }

    Database db {options};
    ASSERT_TRUE(db.open().is_ok());
    ASSERT_EQ(db.info().record_count(), 0);
}

} // <anonymous>
