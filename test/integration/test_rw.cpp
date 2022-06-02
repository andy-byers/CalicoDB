#include <gtest/gtest.h>
#include <filesystem>
#include <thread>
#include <vector>
#include "cursor.h"
#include "database.h"
#include "integration.h"

namespace {
using namespace cub;

TEST(ReaderWriterTests, ManyReadersAndWriters)
{
    static constexpr Size NUM_RECORDS_AT_START = 1000;
    static constexpr Size NUM_RECORDS_PER_ROUND = 100;
    static constexpr Size NUM_READERS = 200;
    static constexpr Size NUM_WRITERS = 200;

    Random random {0};
    auto choices = std::string(NUM_READERS, 'r') +
                   std::string(NUM_WRITERS, 'w');
    random.shuffle(choices);

    std::filesystem::remove(TEST_PATH);
    auto db = Database::open(TEST_PATH, {});

    insert_random_records(db, NUM_RECORDS_AT_START, {});

    std::vector<std::thread> threads;
    for (auto choice: choices) {
        if (choice == 'r') {
            threads.emplace_back(reader_task, db.get_cursor());
        } else {
            threads.emplace_back(writer_task, &db, NUM_RECORDS_PER_ROUND, random.next_int(10));
        }
    }

    for (auto &thread: threads)
        thread.join();
}

} // <anonymous>