#include <gtest/gtest.h>
#include <chrono>
#include <filesystem>
#include <thread>
#include <vector>
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
    cursor.find_minimum();

    Size counter {};
    do {
        (void)cursor.value();
        counter++;
    } while (cursor.increment());
    CUB_EXPECT_EQ(counter, expected_size);
    return nullptr;
}

auto writer_task(Database *db, Size n, Size _0_to_10) -> void*
{
    using namespace std::chrono_literals;
    CUB_EXPECT_LE(_0_to_10, 10);
    RecordGenerator::Parameters param;
    param.seed = static_cast<unsigned>(std::chrono::system_clock::now().time_since_epoch().count());
    for (const auto &[key, value]: RecordGenerator::generate(n, param))
        db->insert(_b(key), _b(value));
    if (_0_to_10 == 0)
        db->commit();
    return nullptr;
}

TEST(ReaderWriterTests, ManyReadersAndWriters)
{
    using namespace std::chrono_literals;
    static constexpr Size NUM_RECORDS_AT_START = 1000;
    static constexpr Size NUM_RECORDS_PER_ROUND = 20;
    static constexpr Size NUM_READERS = 50;
    static constexpr Size NUM_WRITERS = 50;

    Random random {0};
    auto choices = std::string(NUM_READERS, 'r') +
                   std::string(NUM_WRITERS, 'w');
    random.shuffle(choices);

    std::filesystem::remove(TEST_PATH);
    auto db = Database::open(TEST_PATH, {});

    insert_random_records(db, NUM_RECORDS_AT_START, {});

    std::vector<std::thread> threads;
    for (auto choice: choices) {
        std::this_thread::sleep_for(2ms);

        if (choice == 'r') {
            threads.emplace_back(reader_task, &db);
        } else {
            threads.emplace_back(writer_task, &db, NUM_RECORDS_PER_ROUND, random.next_int(10));
        }
    }

    for (auto &thread: threads)
        thread.join();
}

} // <anonymous>