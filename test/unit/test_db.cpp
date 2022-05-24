
#include <array>
#include <filesystem>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "database.h"
#include "cursor.h"
#include "db/database_impl.h"
#include "db/cursor_impl.h"
#include "pool/interface.h"
#include "common.h"
#include "tree/tree.h"

#include "fakes.h"
#include "tools.h"

namespace {

using namespace cub;

constexpr auto TEST_PATH = "/tmp/cub_test";

auto reader_task(Cursor cursor) -> void*
{
    cursor.find_minimum();
    const auto value = cursor.value();
    while (cursor.increment());
    return nullptr;
}

template<class Db> auto insert_random_records(Db &db, Size n)
{
    for (const auto &[key, value]: RecordGenerator::generate(n, {}))
        db.insert(_b(key), _b(value));
}

TEST(DatabaseTest, TestReaders)
{
    static constexpr Size NUM_READERS = 20;
    std::filesystem::remove(TEST_PATH);

    auto db = Database::open(TEST_PATH, {});

    insert_random_records(db, 500);

    std::vector<std::thread> readers;
    while (readers.size() < NUM_READERS)
        readers.emplace_back(reader_task, db.get_cursor());

    for (auto &reader: readers)
        reader.join();
}

auto collect_records(Database &db)
{
    std::vector<Record> records;
    auto cursor = db.get_cursor();

    if (!cursor.has_record())
        return records;

    do {
        records.emplace_back(Record {_s(cursor.key()), cursor.value()});
    } while (cursor.increment());

    return records;
}

TEST(DatabaseTests, DataPersists)
{
    std::vector<Record> records;
    std::filesystem::remove(TEST_PATH);

    {
        auto db = Database::open(TEST_PATH, {});
        insert_random_records(db, 500);
        records = collect_records(db);
    }

    auto db = Database::open(TEST_PATH, {});
    auto cursor = db.get_cursor();
    for (const auto &[key, value]: records) {
        ASSERT_EQ(key, _s(cursor.key()));
        ASSERT_EQ(value, cursor.value());
    }
}

//TEST(DatabaseTests, TestRecovery)
//{
//    FaultyDatabase db;
//    {
//        auto faulty = FaultyDatabase::create(0x200);
//        insert_random_records(*faulty.db, 500);
//        faulty.db->commit();
//        faulty.tree_faults.set_read_fault_rate(100);
//
//        try {
//            auto cursor = faulty.db->get_cursor();
//            while (cursor.increment());
//            ADD_FAILURE() << "Reading from the database file should have thrown";
//        } catch (const IOError&) {
//            faulty.tree_faults.set_read_fault_rate(0);
//        }
//        db = faulty.clone();
//    }
//}

} // <anonymous>