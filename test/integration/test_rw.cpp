/*
* test_rw.cpp: Reader/writer synchronization tests. These should be run with TSan.
*/

#include <chrono>
#include <filesystem>
#include <shared_mutex>
#include <thread>
#include <vector>
#include <gtest/gtest.h>
#include "calico/cursor.h"
#include "calico/database.h"
#include "utils/expect.h"
#include "tools.h"

namespace {
using namespace calico;

constexpr auto TEST_PATH = "/tmp/calico_test";

auto reader_task(Database *db) -> void*
{
    auto cursor = db->get_cursor();
    const auto expected_size = db->get_info().record_count();
    CALICO_EXPECT_GT(expected_size, 1);
    cursor.find_minimum();
    const auto value = cursor.value();
    Size counter {1};

    while (cursor.increment()) {
        // We should be able to call the read*() methods from many threads.
        CALICO_EXPECT_EQ(db->read(cursor.key(), Ordering::EQ)->value, value);
        CALICO_EXPECT_EQ(cursor.value(), value);
        counter++;
    }
    CALICO_EXPECT_EQ(counter, expected_size);
    return nullptr;
}

auto locked_reader_task(Database *db, std::shared_mutex *mutex) -> void*
{
    std::shared_lock lock {*mutex};
    return reader_task(db);
}

auto writer_task(Database *db, std::shared_mutex *mutex, const std::vector<Record> &original) -> void*
{
    std::unique_lock lock {*mutex};
    const auto value = db->read_minimum()->value;
    for (const auto &[key, unused]: original)
        db->write(stob(key), stob(value));
    return nullptr;
}

struct SetupResults {
    std::string choices;
    Database db;
    std::vector<Record> records;
};

auto setup(Size num_readers, Size num_writers)
{
    static constexpr Size NUM_RECORDS_AT_START = 1'000;
    Random random {0};

    auto choices = std::string(num_readers, 'r') +
                   std::string(num_writers, 'w');
    random.shuffle(choices);

    std::filesystem::remove(TEST_PATH);
    auto db = Database::open(TEST_PATH, {});
    DatabaseBuilder builder {&db};
    builder.write_unique_records(NUM_RECORDS_AT_START, {});
    auto records = builder.collect_records();

    // Run once to make all values the same.
    std::shared_mutex mutex;
    writer_task(&db, &mutex, records);

    return SetupResults {
        std::move(choices),
        std::move(db),
        std::move(records),
    };
}

TEST(ReaderWriterTests, ManyReaders)
{
    static constexpr Size NUM_READERS = 250;
    using namespace std::chrono_literals;
    auto [choices, db, records] = setup(NUM_READERS, 0);

    std::vector<std::thread> threads;
    for (auto choice: choices) {
        threads.emplace_back(reader_task, &db);
        CALICO_EXPECT_EQ(choice, 'r');
    }

    for (auto &thread: threads)
        thread.join();
}

TEST(ReaderWriterTests, ManyReadersAndWriters)
{
    static constexpr Size NUM_READERS = 50;
    static constexpr Size NUM_WRITERS = 50;
    using namespace std::chrono_literals;
    auto [choices, db, records] = setup(NUM_READERS, NUM_WRITERS);

    std::shared_mutex mutex;
    std::vector<std::thread> threads;
    for (auto choice: choices) {
        std::this_thread::sleep_for(2ms);

        if (choice == 'r') {
            threads.emplace_back(locked_reader_task, &db, &mutex);
        } else {
            threads.emplace_back(writer_task, &db, &mutex, records);
        }
    }

    for (auto &thread: threads)
        thread.join();
}

} // <anonymous>