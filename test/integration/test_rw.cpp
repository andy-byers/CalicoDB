/*
* test_rw.cpp: Reader/writer synchronization tests. These should be run with TSan.
*/

#include <chrono>
#include <filesystem>
#include <mutex>
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

constexpr auto TEST_PATH = "/tmp/__calico_rw_tests";

auto reader_task(Database *db) -> void*
{
    const auto expected_size = db->info().record_count();
    CALICO_EXPECT_GT(expected_size, 1);
    auto c = db->find_minimum();
    CALICO_EXPECT_TRUE(c.is_valid());
    const auto value = c.value();
    Size counter {};

    for (; c.is_valid(); c++, counter++) {
        // We should be able to call the read*() methods from many threads.
        CALICO_EXPECT_EQ(db->find(c.key()).value(), value);
        CALICO_EXPECT_EQ(c.value(), value);
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
    for (const auto &[key, old_value]: original) {
        const auto value = old_value + old_value;
        db->insert(stob(key), stob(value));
    }
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

    std::error_code ignore;
    std::filesystem::remove_all(TEST_PATH, ignore);

    auto db = Database::open(TEST_PATH, {});
    for (Index i {}; i < NUM_RECORDS_AT_START; ++i)
        db.insert({std::to_string(i), "<CALICO>"});

    std::vector<Record> records;
    records.reserve(db.info().record_count());
    for (auto c = db.find_minimum(); c.is_valid(); c++)
        records.emplace_back(c.record());

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