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

//auto append_commit_record_to_old_wal(Size block_size)
//{
//    auto wal_reader_file = std::make_unique<ReadOnlyFile>(get_wal_path(PATH), Mode::DIRECT, 0666);
//    auto wal_writer_file = std::make_unique<LogFile>(get_wal_path(PATH), Mode::DIRECT, 0666);
//    auto wal_reader = std::make_unique<WALReader>(std::move(wal_reader_file), block_size);
//    auto wal_writer = std::make_unique<WALWriter>(std::move(wal_writer_file), block_size);
//    wal_reader->reset();
//    CUB_EXPECT_NE(wal_reader->record(), std::nullopt);
//
//    // Find the LSN of the last WAL record.
//    while (wal_reader->increment());
//    auto last_lsn = wal_reader->record()->lsn();
//    CUB_EXPECT_GT(last_lsn.value, 0);
//
//    // Write a commit record. Now we can pretend we failed right after we flushed the commit WAL record, but
//    // before we were able to flush all the dirty buffer pool pages.
//    last_lsn++;
//    auto rec = WALRecord::commit(last_lsn);
//    std::cout << rec.is_commit() << ' '<<std::hex << rec.crc() << '\n';
//    wal_writer->write(rec);
//    wal_writer->flush();
//}

auto setup(Options options, Size num_records)
{
    std::vector<Record> records;
    std::filesystem::remove(PATH);
    std::filesystem::remove(get_wal_path(PATH));
    auto old_db = Database::open(PATH, options);

    // This batch should be committed to the database.
    insert_random_unique_records(old_db, num_records);
    old_db.commit();
    records = collect_records(old_db);

    // This batch should not be present after recovery.
    insert_random_unique_records(old_db, num_records);

    try {
        // The database uses file descriptors 3, 4, and 5 (if none were open on startup). We should be able to
        // simulate failure by closing its file descriptors.
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

TEST(RecoveryTests, RollsBackToPreviousCommit)
{
    static constexpr Size NUM_RECORDS = 5000;
    Options options;
    options.page_size = 0x200;
    options.block_size = 0x200;

    auto [db, records] = setup(options, NUM_RECORDS);
    validate(db, records);
}

} // <anonymous>