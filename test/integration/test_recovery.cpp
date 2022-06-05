#include <filesystem>
#include <vector>
#include "cursor.h"
#include "database.h"
#include "integration.h"
#include "tools.h"
#include "file/file.h"
#include "file/system.h"
#include "wal/wal_reader.h"
#include "wal/wal_record.h"
#include "wal/wal_writer.h"

namespace {
using namespace cub;

constexpr auto PATH = "/tmp/cub_recovery";

struct SetupResults {
    Database db;
    std::vector<Record> committed;
};

auto validate(Database &db, const std::vector<Record> &records)
{
    auto cursor = db.get_cursor();
    for (const auto &[key, value]: records) {
        ASSERT_TRUE(cursor.find(_b(key)));
        ASSERT_EQ(_s(cursor.key()), key);
        ASSERT_EQ(cursor.value(), value);
    }
    const auto info = db.get_info();
    ASSERT_EQ(info.record_count(), records.size());
}

auto setup(Options options, Size num_records)
{
    std::vector<Record> records;
    std::filesystem::remove(PATH);
    std::filesystem::remove(get_wal_path(PATH));
    auto old_db = Database::open(PATH, options);

//    RecordGenerator::Parameters param;
//    param.min_key_size = 15;
//    param.max_key_size = 20;
//    param.min_value_size = options.page_size / 4;
//    param.max_value_size = options.page_size / 3;

    // This batch should be committed to the database.
//    insert_random_records(old_db, num_records, param);
    insert_random_unique_records(old_db, num_records);
    old_db.commit();
    records = collect_records(old_db);

    // This batch should not be present after recovery.
//    insert_random_records(old_db, num_records, param);
    insert_random_unique_records(old_db, num_records);

    try {
        // The database uses file descriptors 3, 4, and 5 (if none were open on startup). We should be able to   TODO: So sketchy!
        // simulate failure by closing its file descriptors. Since the WAL is flushed on-demand, we may be left
        // with an incomplete record at the end.
        system::close(3);
        system::close(4);
        system::close(5);
    } catch (...) {
        ADD_FAILURE() << "Unable to close database files";
    }

    try {
        auto cursor = old_db.get_cursor();
        while (cursor.increment());
        ADD_FAILURE() << "Reading from the database file should have failed";
    } catch (...) {

    }

    old_db.~Database();

    // Now we can open a new database and recover.
    return SetupResults {Database::open(PATH, options), records};
}

TEST(RecoveryTests, RollsBackToPreviousCommit)
{
    static constexpr Size NUM_RECORDS = 2500;
    Options options;
    options.frame_count = 16;
    options.page_size = 0x100;
    options.block_size = 0x100;

    auto [db, records] = setup(options, NUM_RECORDS);
    validate(db, records);
}

} // <anonymous>