
// TODO: Have to refactor these tests, we no longer pass in a custom WAL object to the database constructor.

//#include <fstream>
//#include <gtest/gtest.h>
//
//#include "calico/bytes.h"
//#include "calico/options.h"
//#include "calico/store.h"
//#include "fakes.h"
//#include "pager/basic_pager.h"
//#include "pager/framer.h"
//#include "store/disk.h"
//#include "store/system.h"
//#include "tools.h"
//#include "unit_tests.h"
//#include "utils/layout.h"
//#include "utils/logging.h"
//#include "utils/utils.h"
//#include "wal/basic_wal.h"
//#include "wal/helpers.h"
//#include "wal/reader.h"
//#include "wal/writer.h"
//
//namespace {
//
//using namespace calico;
//namespace fs = std::filesystem;
//
//class XactTests: public TestOnDisk {
//public:
//    auto SetUp() -> void override
//    {
//        WriteAheadLog *temp {};
//        BasicWriteAheadLog::Parameters param {
//            ROOT,
//            store.get(),
//            create_sink(ROOT, spdlog::level::trace),
//            0x200,
//            128,
//        };
//        ASSERT_TRUE(expose_message(BasicWriteAheadLog::open(param, &temp)));
//        wal.reset(temp);
//
//        options.page_size = 0x400;
//        options.frame_count = 64;
//        options.log_level = spdlog::level::trace;
//        options.store = store.get();
//
//        ASSERT_TRUE(expose_message(db.open(ROOT, options)));
//        ASSERT_TRUE(db.is_open());
//    }
//
//    auto TearDown() -> void override
//    {
//        if (db.is_open()) {
//            ASSERT_TRUE(expose_message(db.close()));
//        }
//        ASSERT_FALSE(db.is_open());
//    }
//
//    RecordGenerator generator {{16, 100, 10, false, true}};
//    std::unique_ptr<WriteAheadLog> wal;
//    Random random {123};
//    Options options;
//    Database db;
//};
//
//TEST_F(XactTests, NewDatabaseIsOk)
//{
//    ASSERT_TRUE(expose_message(db.status()));
//}
//
//TEST_F(XactTests, WalIsReadyAfterStartup)
//{
//    ASSERT_TRUE(wal->is_enabled());
//    ASSERT_TRUE(wal->is_writing());
//}
//
//TEST_F(XactTests, CommittingEmptyXactIsOk)
//{
//    ASSERT_TRUE(expose_message(db.commit()));
//}
//
//TEST_F(XactTests, AbortingEmptyXactIsOk)
//{
//    ASSERT_TRUE(expose_message(db.abort()));
//}
//
//auto insert_1000_records(XactTests &test)
//{
//    auto records = test.generator.generate(test.random, 1'000);
//    for (const auto &r: records) {
//        EXPECT_TRUE(expose_message(test.db.insert(r)));
//    }
//    return records;
//}
//
//auto erase_1000_records(XactTests &test)
//{
//    for (Size i {}; i < 1'000; ++i) {
//        ASSERT_TRUE(expose_message(test.db.erase(test.db.find_minimum())));
//    }
//}
//
//TEST_F(XactTests, SequenceIdsAlwaysIncrease)
//{
//    insert_1000_records(*this);
//
//    // The WAL design should allow stopping and starting at any time. We should be able to call redo_all() safely, and the WAL
//    // will just reload the positions of each WAL record in the most-recent transaction. If we want to call undo_last() manually,
//    // we should return a non-OK status from the callback at some point, otherwise the WAL will get rid of the segments belonging to
//    // the most-recent transaction. Normally, we wouldn't want to call either of these methods directly. This is just for testing!
//    SequenceId last_lsn;
//    ASSERT_TRUE(expose_message(wal->stop_writer()));
//    ASSERT_TRUE(expose_message(wal->setup_and_recover(
//        [&last_lsn](const auto &info) {
//            EXPECT_LT(last_lsn, info.page_lsn);
//            return Status::ok();
//        },
//        [](const auto &) {
//            return Status::ok();
//        })));
//    ASSERT_TRUE(expose_message(wal->start_writer()));
//}
//
//TEST_F(XactTests, AbortFirstXact)
//{
//    insert_1000_records(*this);
//    ASSERT_TRUE(expose_message(db.abort()));
//    ASSERT_EQ(db.info().record_count(), 0);
//
//    // Normal operations after abort should work.
//    insert_1000_records(*this);
//    ASSERT_EQ(db.info().record_count(), 1'000);
//}
//
//TEST_F(XactTests, CommitIsACheckpoint)
//{
//    insert_1000_records(*this);
//    ASSERT_TRUE(expose_message(db.commit()));
//    ASSERT_TRUE(expose_message(db.abort()));
//    ASSERT_EQ(db.info().record_count(), 1'000);
//
//    insert_1000_records(*this);
//    ASSERT_TRUE(expose_message(db.abort()));
//    ASSERT_EQ(db.info().record_count(), 1'000);
//}
//
//TEST_F(XactTests, KeepsCommittedRecords)
//{
//    insert_1000_records(*this);
//    ASSERT_TRUE(expose_message(db.commit()));
//    erase_1000_records(*this);
//    ASSERT_TRUE(expose_message(db.abort()));
//    ASSERT_EQ(db.info().record_count(), 1'000);
//
//    // Normal operations after abort should work.
//    erase_1000_records(*this);
//    ASSERT_EQ(db.info().record_count(), 0);
//}
//
//template<class Test, class Itr>
//auto run_random_operations(Test &test, const Itr &begin, const Itr &end)
//{
//    for (auto itr = begin; itr != end; ++itr) {
//        EXPECT_TRUE(expose_message(test.db.insert(*itr)));
//    }
//    std::vector<Record> committed;
//    for (auto itr = begin; itr != end; ++itr) {
//        if (test.random.next_int(5) == 0) {
//            EXPECT_TRUE(expose_message(test.db.erase(itr->key)));
//        } else {
//            committed.emplace_back(*itr);
//        }
//    }
//    return committed;
//}
//
//TEST_F(XactTests, AbortRestoresPriorState)
//{
//    static constexpr Size NUM_RECORDS {500};
//    const auto path = ROOT + std::string {DATA_FILENAME};
//    const auto records = generator.generate(random, NUM_RECORDS);
//
//    auto committed = run_random_operations(*this, cbegin(records), cbegin(records) + NUM_RECORDS/2);
//    ASSERT_TRUE(expose_message(db.commit()));
//
//    run_random_operations(*this, cbegin(records) + NUM_RECORDS/2, cend(records));
//    ASSERT_TRUE(expose_message(db.abort()));
//
//    // The database should contain exactly these records.
//    ASSERT_EQ(db.info().record_count(), committed.size());
//    for (const auto &[key, value]: committed) {
//        ASSERT_TRUE(tools::contains(db, key, value));
//    }
//}
//
//template<class Test>
//[[nodiscard]]
//auto run_random_transactions(Test &test, Size n)
//{
//    static constexpr long XACT_SIZE {100};
//    // Generate the records all at once, so we know that they are unique.
//    auto all_records = test.generator.generate(test.random, n * XACT_SIZE);
//    std::vector<Record> committed;
//
//    for (Size i {}; i < n; ++i) {
//        const auto start = cbegin(all_records) + static_cast<long>(XACT_SIZE * i);
//        const auto temp = run_random_operations(test, start, start + XACT_SIZE);
//        if (test.random.next_int(4UL) == 0) {
//            EXPECT_TRUE(expose_message(test.db.abort()));
//        } else {
//            EXPECT_TRUE(expose_message(test.db.commit()));
//            committed.insert(cend(committed), cbegin(temp), cend(temp));
//        }
//    }
//    return committed;
//}
//
//template<class Test>
//auto test_transactions(Test &test, Size n)
//{
//    for (const auto &[key, value]: run_random_transactions(test, n)) {
//        ASSERT_TRUE(tools::contains(test.db, key, value));
//    }
//}
//
//TEST_F(XactTests, SanityCheck)
//{
//    test_transactions(*this, 20);
//}
//
//TEST_F(XactTests, PersistenceSanityCheck)
//{
//    ASSERT_TRUE(expose_message(db.close()));
//    ASSERT_TRUE(expose_message(db.open(ROOT, options)));
//
////    ASSERT_TRUE(expose_message(db.close()));
////
////    for (Size i {}; i < 5; ++i) {
////        ASSERT_TRUE(expose_message(db.open(ROOT, options)));
////        test_transactions(*this, 3);
////        ASSERT_TRUE(expose_message(db.close()));
////    }
//}
////
////class IncompleteXactTests: public TestOnHeap {
////public:
////    static constexpr Size PAGE_SIZE {0x200};
////
////    IncompleteXactTests() = default;
////
////    ~IncompleteXactTests() override = default;
////
////    auto SetUp() -> void override
////    {
////        WriteAheadLog *temp {};
////        BasicWriteAheadLog::Parameters param {
////            ROOT,
////            store.get(),
////            create_sink(),
////            PAGE_SIZE,
////        };
////        ASSERT_TRUE(expose_message(BasicWriteAheadLog::open(param, &temp)));
////        wal.reset(temp);
////
////        options.page_size = PAGE_SIZE;
////        options.frame_count = 16;
////        options.log_level = spdlog::level::trace;
////        options.store = store.get();
////        options.wal = wal.get();
////
////        ASSERT_TRUE(expose_message(db.open(ROOT, options)));
////        ASSERT_TRUE(db.is_open());
////
////        const auto records = generator.generate(random, 500);
////        committed = run_random_operations(*this, cbegin(records), cend(records));
////        ASSERT_TRUE(expose_message(db.commit()));
////
//////        const auto uncommitted = generator.generate(random, 500);
//////        run_random_operations(*this, cbegin(records), cend(records));
////
////        auto *cloned = dynamic_cast<HeapStorage &>(*store).clone();
////
////        ASSERT_TRUE(expose_message(db.close()));
////        wal.reset();
////
////        store.reset(cloned);
////        param.store = store.get();
////
////        ASSERT_TRUE(expose_message(BasicWriteAheadLog::open(param, &temp)));
////        wal.reset(temp);
////        options.store = store.get();
////        options.wal = wal.get();
////
////        ASSERT_TRUE(expose_message(db.open(ROOT, options)));
////        ASSERT_TRUE(db.is_open());
////
////        for (const auto &[key, value]: committed)
////            tools::expect_contains(db, key, value);
////
////        ASSERT_EQ(db.info().record_count(), committed.size());
////    }
////
////    auto TearDown() -> void override
////    {
////        if (db.is_open()) {
////            ASSERT_TRUE(expose_message(db.close()));
////        }
////        ASSERT_FALSE(db.is_open());
////    }
////
////    RecordGenerator generator {{16, 100, 10, false, true}};
////    std::unique_ptr<WriteAheadLog> wal;
////    std::vector<Record> committed;
////    Random random {123};
////    Options options;
////    Database db;
////};
////
////TEST_F(IncompleteXactTests, X)
////{
////
////}
//
//} // <anonymous>