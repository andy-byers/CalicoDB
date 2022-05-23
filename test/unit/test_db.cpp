
#include <array>
#include <filesystem>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "database.h"
#include "cursor.h"
#include "db/database_impl.h"
#include "db/cursor_impl.h"
#include "pool/buffer_pool.h"
#include "pool/frame.h"
#include "pool/interface.h"
#include "page/cell.h"
#include "common.h"
#include "utils/layout.h"
#include "tree/tree.h"

#include "random.h"
#include "tools.h"

namespace {

using namespace cub;

constexpr auto TEST_PATH = "/tmp/cub_test";

auto reader_task(Cursor cursor) -> void*
{
    cursor.find_minimum();
    const auto value = cursor.value();
    while (cursor.increment())
        puts(cursor.value().c_str());
//        CUB_EXPECT_EQ(cursor.value(), value);
    return nullptr;
}

TEST(DatabaseTest, TestReaders)
{
    static constexpr Size NUM_READERS = 100;
    std::filesystem::remove(TEST_PATH);

    auto db = Database::open(TEST_PATH, {});

    for (const auto &[key, value]: RecordGenerator::generate(1000, {}))
        db.insert(_b(key), _b(value));

    std::vector<std::thread> readers;
    while (readers.size() < NUM_READERS)
        readers.emplace_back(reader_task, db.get_cursor());

    for (auto &reader: readers)
        reader.join();
}

//TEST(DatabaseTest, TestReaders)
//{
//    std::filesystem::remove("/tmp/cub_test");
//
//    auto db = Database::open("/tmp/cub_test", {});
//    {
//        auto writer = db.get_writer();
//        writer.insert(_b("a"), _b("1"));
//        writer.insert(_b("b"), _b("2"));
//        writer.insert(_b("c"), _b("3"));
//    }
//    {
//        auto reader_1 = db.get_reader();
//        auto reader_2 = db.get_reader();
//        auto reader_3 = db.get_reader();
//        reader_2.increment();
//        reader_3.increment(2);
//        ASSERT_EQ(_s(reader_1.key()), "a");
//        ASSERT_EQ(_s(reader_2.key()), "b");
//        ASSERT_EQ(_s(reader_3.key()), "c");
//        ASSERT_EQ(reader_1.value(), "1");
//        ASSERT_EQ(reader_2.value(), "2");
//        ASSERT_EQ(reader_3.value(), "3");
//    }
//}
//
//auto reader_task(Reader reader, bool *failed) -> void*
//{
//    (void)reader;
//    (void)failed;
//    reader.find_minimum();
//    const auto value = reader.value();
//    while (reader.increment()) {
//        if (reader.value() != value) {
//            *failed = true; // TODO: Maybe will trip up TSan.
//            break;
//        }
//    }
//    puts("reader finished");
//    return nullptr;
//}
//
//auto writer_task(Writer writer) -> void*
//{
//    (void)writer;
//
//    writer.find_minimum();
//    while (writer.increment()) {
//        const auto new_value = std::to_string(std::stoi(writer.value()) + 1);
//        writer.modify(_b(new_value));
//    }
//    puts("writer finished");
//    return nullptr;
//}
//
//[[maybe_unused]] auto populate(Writer writer, Size n)
//{
//    for (Index i {}; i < n; ++i) {
//        const auto key = std::to_string(i);
//        writer.insert(_b(key), _b("0"));
//    }
//}
//
//TEST(ConsistencyTest, ReadersAndWriters)
//{
//    static constexpr Size total_num_readers = 0;
//    static constexpr Size total_num_writers = 1;
//
//    std::filesystem::remove("/tmp/cub_test");
//
//    Random random {0};
//    bool had_failure {};
//    auto db = Database::open("/tmp/cub_test", {});
//
//
//    auto choices = std::string(total_num_readers, 'R') +
//                   std::string(total_num_writers, 'W');
//    random.shuffle(choices);
//
//    {
//        populate(db.get_writer(), 200);
//    }
//
//    std::vector<std::thread> threads;
//    for (auto choice: choices) {
//        if (choice == 'R') {
//            threads.emplace_back(reader_task, db.get_reader(), &had_failure);
//        } else {
//            threads.emplace_back(writer_task, db.get_writer());
//        }
//    }
//    ASSERT_FALSE(had_failure);
//
//    for (auto &thread: threads)
//        thread.join();
//}

} // <anonymous>