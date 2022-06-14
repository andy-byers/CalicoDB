/*
* test_rw.cpp: Reader/writer synchronization tests. These should be run with TSan.
*/

#include <chrono>
#include <filesystem>
#include <thread>
#include <vector>
#include <gtest/gtest.h>
#include "cub/cursor.h"
#include "cub/database.h"
#include "utils/expect.h"
#include "tools.h"

namespace {
using namespace cub;

constexpr auto TEST_PATH = "/tmp/cub_test";

auto reader_task(Database *db) -> void*
{
    auto cursor = db->get_cursor();
    const auto expected_size = db->get_info().record_count();
    CUB_EXPECT_GT(expected_size, 1);
    cursor.find_minimum();
    const auto value = cursor.value();
    Size counter {1};
    while (cursor.increment()) {
        CUB_EXPECT_EQ(cursor.value(), value);
        counter++;
    }
    CUB_EXPECT_EQ(counter, expected_size);
    return nullptr;
}

auto writer_task(Database *db, const std::vector<Record> &original) -> void*
{
    auto lock = db->get_lock();
    const auto value = std::to_string(rand());
    for (const auto &[key, unused]: original)
        db->write(_b(key), _b(value));
    return nullptr;
}

TEST(ReaderWriterTests, ManyReadersAndWriters)
{
    using namespace std::chrono_literals;
    static constexpr Size NUM_RECORDS_AT_START = 500;
    static constexpr Size NUM_READERS = 50;
    static constexpr Size NUM_WRITERS = 50;

    Random random {0};
    auto choices = std::string(NUM_READERS, 'r') +
                   std::string(NUM_WRITERS, 'w');
    random.shuffle(choices);

    std::filesystem::remove(TEST_PATH);
    auto db = Database::open(TEST_PATH, {});
    DatabaseBuilder builder {&db};
    builder.write_unique_records(NUM_RECORDS_AT_START, {});
    const auto records = builder.collect_records();

    // Run once to make all values the same.
    writer_task(&db, records);

    std::vector<std::thread> threads;
    for (auto choice: choices) {
        std::this_thread::sleep_for(2ms);

        if (choice == 'r') {
            threads.emplace_back(reader_task, &db);
        } else {
            threads.emplace_back(writer_task, &db, records);
        }
    }

    for (auto &thread: threads)
        thread.join();
}

} // <anonymous>