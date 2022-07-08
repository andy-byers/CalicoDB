
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

using namespace calico;
namespace fs = std::filesystem;
constexpr auto BASE = "/tmp/__calico_database_tests";

class DatabaseTests: public testing::Test {
public:
    DatabaseTests()
    {
        options.page_size = 0x200;
        options.block_size = 0x200;

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
        auto db = Database::open(BASE, options);

        for (Index i {}; i < GROUP_SIZE; ++i) {
            db.insert(*itr);
            itr++;
        }
    }

    auto db = Database::open(BASE, options);
    CALICO_EXPECT_EQ(db.info().record_count(), records.size());
    for (const auto &[key, value]: records) {
        const auto c = tools::find_exact(db, key);
        ASSERT_TRUE(c.is_valid());
        ASSERT_EQ(btos(c.key()), key);
        ASSERT_EQ(c.value(), value);
    }
}

TEST_F(DatabaseTests, SanityCheck)
{
    static constexpr Size NUM_ITERATIONS {5};
    static constexpr Size GROUP_SIZE {500};
    Options options;
    options.page_size = 0x200;
    options.block_size = 0x200;

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
        auto db = Database::open(BASE, options);

        for (const auto &record: generator.generate(random, GROUP_SIZE))
            db.insert(record);
    }

    for (Index iteration {}; iteration < NUM_ITERATIONS; ++iteration) {
        auto db = Database::open(BASE, options);

        for (const auto &record: generator.generate(random, GROUP_SIZE))
            tools::erase_one(db, record.key);
    }

    auto db = Database::open(BASE, options);
    CALICO_EXPECT_EQ(db.info().record_count(), 0);
}

auto get_faulty_database()
{
    Options options;
    options.frame_count = 32;
    options.page_size = 0x200;
    options.block_size = 0x200;
    return FakeDatabase {options};
}

class DatabaseFaultTests: public testing::Test {
public:
    DatabaseFaultTests()
        : db {get_faulty_database()}
    {
        RecordGenerator::Parameters param;
        param.mean_key_size = 20;
        param.mean_value_size = 20;
        param.spread = 15;
        generator = RecordGenerator {param};

        committed = generator.generate(random, 1'000);
        for (const auto &[key, value]: committed)
            tools::insert(*db.db, key, value);
        db.db->commit();

        for (const auto &[key, value]: generator.generate(random, 1'000))
            tools::insert(*db.db, key, value);
    }

    ~DatabaseFaultTests() override
    {
        for (const auto &[key, value]: committed) {
            auto c = tools::find_exact(*db.db, key);
            EXPECT_EQ(c.value(), value);
        }
        EXPECT_EQ(db.db->info().record_count(), committed.size());
    }

    template<class Action, class EnableFaults, class DisableFaults>
    auto run_test(const Action &action, const EnableFaults &enable, const DisableFaults &disable)
    {
        for (int i {}; i < 10; ++i) {
            try {
                enable();
                action();
                ADD_FAILURE() << "action() should have thrown an exception";
            } catch (const std::exception &error) {
                disable();
            }
        }
        action();
    }

    Random random {0};
    RecordGenerator generator;
    FakeDatabase db;
    std::vector<Record> committed;
};

TEST_F(DatabaseFaultTests, AbortDataFaults)
{
    run_test([&] {db.db->abort();},
             [&] {db.data_faults.set_read_fault_rate(10);
                               db.data_faults.set_write_fault_rate(10);},
             [&] {db.data_faults.set_read_fault_rate(0);
                               db.data_faults.set_write_fault_rate(0);});
}

TEST_F(DatabaseFaultTests, AbortWALFaults)
{
    run_test([&] {db.db->abort();},
             [&] {db.wal_faults.set_read_fault_rate(10);
                               db.wal_faults.set_write_fault_rate(10);},
             [&] {db.wal_faults.set_read_fault_rate(0);
                               db.wal_faults.set_write_fault_rate(0);});
}

} // <anonymous>
