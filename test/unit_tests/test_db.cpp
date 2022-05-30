
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

#include "file/file.h"

namespace {

using namespace cub;

constexpr auto TEST_PATH = "/tmp/cub_test";

TEST(DatabaseTests, DataPersists)
{
    std::vector<Record> records;
    std::filesystem::remove(TEST_PATH);

    {
        auto db = Database::open(TEST_PATH, {});
        insert_random_records(db, 500, {});
        records = collect_records(db);
        ASSERT_EQ(db.get_info().record_count(), records.size());
    }

    auto db = Database::open(TEST_PATH, {});
    ASSERT_EQ(db.get_info().record_count(), records.size());
    auto cursor = db.get_cursor();
    for (const auto &[key, value]: records) {
        ASSERT_TRUE(cursor.find(_b(key)));
        ASSERT_EQ(key, _s(cursor.key()));
        ASSERT_EQ(value, cursor.value());
    }
}

TEST(DatabaseTests, TestRecovery)
{
    static constexpr Size n = 1000;
    std::vector<Record> records;
    FaultyDatabase db;
    {
        auto faulty = FaultyDatabase::create(0x200);
        insert_random_records(*faulty.db, n, {});
        faulty.db->commit();
        records = collect_records(*faulty.db);
        insert_random_records(*faulty.db, n, {});

        try {
            // Sketchy way to attempt to delay the exception for a few reads/writes.
            faulty.tree_faults.set_write_fault_rate(10);
            faulty.db->commit();
            ADD_FAILURE() << "commit() should have thrown";
        } catch (const IOError&) {

        }
        db = faulty.clone();
    }
    auto cursor = db.db->get_cursor();
    for (const auto &[key, value]: records) {
        ASSERT_TRUE(cursor.find(_b(key)));
        ASSERT_EQ(_s(cursor.key()), key);
        ASSERT_EQ(cursor.value(), value);
    }
}

} // <anonymous>