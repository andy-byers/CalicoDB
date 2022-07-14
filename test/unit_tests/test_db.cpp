
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

class DatabaseTests: public testing::Test {
public:
    DatabaseTests()
    {
        options.page_size = 0x200;

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
        auto db = *Database::open(BASE, options);

        for (Index i {}; i < GROUP_SIZE; ++i) {
            db.insert(*itr);
            itr++;
        }
    }

    auto db = *Database::open(BASE, options);
    CCO_EXPECT_EQ(db.info().record_count(), records.size());
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
    static constexpr Size GROUP_SIZE {5'000};
    Options options;
    options.page_size = 0x200;

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
        auto db = *Database::open(BASE, options);

        for (const auto &record: generator.generate(random, GROUP_SIZE))
            db.insert(record);
    }

    for (Index iteration {}; iteration < NUM_ITERATIONS; ++iteration) {
        auto db = *Database::open(BASE, options);

        for (const auto &record: generator.generate(random, GROUP_SIZE))
            tools::erase_one(db, record.key);
    }

    auto db = *Database::open(BASE, options);
    CCO_EXPECT_EQ(db.info().record_count(), 0);
}

} // <anonymous>
