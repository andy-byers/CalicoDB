#include <fstream>
#include <gtest/gtest.h>

#include "calico/bytes.h"
#include "calico/options.h"
#include "calico/store.h"
#include "fakes.h"
#include "pager/basic_pager.h"
#include "pager/framer.h"
#include "store/disk.h"
#include "store/system.h"
#include "tools.h"
#include "unit_tests.h"
#include "utils/layout.h"
#include "utils/logging.h"
#include "utils/utils.h"
#include "wal/basic_wal.h"
#include "wal/helpers.h"
#include "wal/reader.h"
#include "wal/writer.h"

namespace {

using namespace calico;
namespace fs = std::filesystem;

class XactTests: public testing::Test {
public:
    static constexpr auto ROOT = "/tmp/__calico_xact_tests/";

    XactTests()
    {
        std::error_code ignore;
        fs::remove_all(ROOT, ignore);
    }

    ~XactTests() override
    {
//        std::error_code ignore;
//        fs::remove_all(ROOT, ignore);
    }

    auto SetUp() -> void override
    {
        store = std::make_unique<DiskStorage>();
        store->create_directory(ROOT);

        WriteAheadLog *temp {};
        BasicWriteAheadLog::Parameters param {
            ROOT,
            store.get(),
            create_sink(ROOT, spdlog::level::trace),
            0x200,
        };
        ASSERT_TRUE(expose_message(BasicWriteAheadLog::open(param, &temp)));
        wal.reset(temp);

        options.page_size = 0x400;
        options.frame_count = 64;
        options.log_level = spdlog::level::trace;
        options.store = store.get();
        options.wal = wal.get();

        ASSERT_TRUE(expose_message(db.open(ROOT, options)));
        ASSERT_TRUE(db.is_open());
    }

    auto TearDown() -> void override
    {
        if (db.is_open()) {
            ASSERT_TRUE(expose_message(db.close()));
        }
        ASSERT_FALSE(db.is_open());
    }

    RecordGenerator generator {{16, 100, 10, false, true}};
    std::unique_ptr<Storage> store;
    std::unique_ptr<WriteAheadLog> wal;
    Random random {123};
    Options options;
    Database db;
};

TEST_F(XactTests, NewDatabaseIsOk)
{
    ASSERT_TRUE(expose_message(db.status()));
}

TEST_F(XactTests, WalIsReadyAfterStartup)
{
    ASSERT_TRUE(wal->is_enabled());
    ASSERT_TRUE(wal->is_writing());
}

TEST_F(XactTests, CommittingEmptyXactIsOk)
{
    ASSERT_TRUE(expose_message(db.commit()));
}

TEST_F(XactTests, AbortingEmptyXactIsOk)
{
    ASSERT_TRUE(expose_message(db.abort()));
}

auto insert_1000_records(XactTests &test)
{
    auto records = test.generator.generate(test.random, 1'000);
    for (const auto &r: records) {
        EXPECT_TRUE(expose_message(test.db.insert(r)));
    }
    return records;
}

auto erase_1000_records(XactTests &test)
{
    for (Size i {}; i < 1'000; ++i) {
        ASSERT_TRUE(expose_message(test.db.erase(test.db.find_minimum())));
    }
}

TEST_F(XactTests, SequenceIdsAlwaysIncrease)
{
    insert_1000_records(*this);

    // The WAL design should allow stopping and starting at any time. We should be able to call redo_all() safely, and the WAL
    // will just reload the positions of each WAL record in the most-recent transaction. If we want to call undo_last() manually,
    // we should return a non-OK status from the callback at some point, otherwise the WAL will get rid of the segments belonging to
    // the most-recent transaction. Normally, we wouldn't want to call either of these methods directly. This is just for testing!
    SequenceId last_lsn;
    ASSERT_TRUE(expose_message(wal->stop_writer()));
    ASSERT_TRUE(expose_message(wal->open_and_recover(
        [&last_lsn](const auto &info) {
            EXPECT_LT(last_lsn, info.page_lsn);
            return Status::ok();
        },
        [](const auto &) {
            return Status::ok();
        })));
    ASSERT_TRUE(expose_message(wal->start_writer()));
}

TEST_F(XactTests, AbortFirstXact)
{
    insert_1000_records(*this);
    ASSERT_TRUE(expose_message(db.abort()));
    ASSERT_EQ(db.info().record_count(), 0);

    // Normal operations after abort should work.
    insert_1000_records(*this);
    ASSERT_EQ(db.info().record_count(), 1'000);
}

TEST_F(XactTests, CommitIsACheckpoint)
{
    insert_1000_records(*this);
    ASSERT_TRUE(expose_message(db.commit()));
    ASSERT_TRUE(expose_message(db.abort()));
    ASSERT_EQ(db.info().record_count(), 1'000);

    insert_1000_records(*this);
    ASSERT_TRUE(expose_message(db.abort()));
    ASSERT_EQ(db.info().record_count(), 1'000);
}

TEST_F(XactTests, KeepsCommittedRecords)
{
    insert_1000_records(*this);
    ASSERT_TRUE(expose_message(db.commit()));
    erase_1000_records(*this);
    ASSERT_TRUE(expose_message(db.abort()));
    ASSERT_EQ(db.info().record_count(), 1'000);

    // Normal operations after abort should work.
    erase_1000_records(*this);
    ASSERT_EQ(db.info().record_count(), 0);
}

template<class Test, class Itr>
auto run_random_operations(Test &test, const Itr &begin, const Itr &end)
{
    for (auto itr = begin; itr != end; ++itr) {
        EXPECT_TRUE(expose_message(test.db.insert(*itr)));
    }
    std::vector<Record> committed;
    for (auto itr = begin; itr != end; ++itr) {
        if (test.random.next_int(5) == 0) {
            EXPECT_TRUE(expose_message(test.db.erase(itr->key)));
        } else {
            committed.emplace_back(*itr);
        }
    }
    return committed;
}

[[nodiscard]]
auto read_whole_file(const std::string &path) -> std::string
{
    std::string data;
    std::ifstream ifs {path};
    EXPECT_TRUE(ifs.is_open());
    ifs >> data;
    return data;
}

TEST_F(XactTests, AbortSanityCheck)
{
    DataFileInspector inspector {std::string {ROOT} + DATA_FILENAME, db.info().page_size()};

    for (Size i {}; i < 3; ++i) {
        insert_1000_records(*this);
//        ASSERT_TRUE(expose_message(db.abort()));
//        insert_1000_records(*this);
//        erase_1000_records(*this);
        ASSERT_TRUE(expose_message(db.abort()));

        ASSERT_EQ(db.info().record_count(), 0);

        auto root = inspector.get_page(PageId::root());
        const auto offset = sizeof(FileHeader) + PageLayout::HEADER_SIZE + NodeHeader::cell_directory_offset(root);
        const auto content = root.view(offset);
        ASSERT_EQ(root.type(), PageType::EXTERNAL_NODE);
        ASSERT_EQ(root.lsn(), SequenceId::null());
        ASSERT_TRUE(std::all_of(content.data(), content.data() + content.size(), [](auto c) {
            return c == '\x00';
        }));
    }
    ASSERT_EQ(db.info().record_count(), 0);
}

TEST_F(XactTests, AbortRestoresPriorState)
{
    const auto path = ROOT + std::string{"/data"};
    const auto before = read_whole_file(path);
    const auto records = generator.generate(random, 500);
    for (const auto &r: run_random_operations(*this, cbegin(records), cend(records))) {
        ASSERT_TRUE(tools::contains(db, r.key));
    }
    ASSERT_TRUE(expose_message(db.abort()));
    const auto after = read_whole_file(path);

    // TODO: Pager component should truncate the data file given the new page_count value after the abort. The second argument passed to substr() below should be removed once this is implemented.
    ASSERT_EQ(before.substr(sizeof(FileHeader)), after.substr(sizeof(FileHeader), before.size() - sizeof(FileHeader)));
}

template<class Test>
[[nodiscard]]
auto run_random_transactions(Test &test, Size n)
{
    {
        DataFileInspector inspector {std::string {Test::ROOT} + DATA_FILENAME, test.db.info().page_size()};
        const auto page = inspector.get_page(PageId::root());
        hexdump(page.view(0).data(), page.size());
        fmt::print("\n");
    }

    static constexpr Size XACT_SIZE {1};

    // Generate the records all at once, so we know that they are unique.
    auto all_records = test.generator.generate(test.random, n * XACT_SIZE);
    std::vector<Record> committed;

    for (Size i {}; i < n; ++i) {
        const auto start = cbegin(all_records) + long(XACT_SIZE*i);
        const auto temp = run_random_operations(test, start, start + XACT_SIZE);
//        if (test.random.next_int(4UL) == 0) {
//            EXPECT_TRUE(expose_message(test.db.abort()));
//
//            DataFileInspector inspector {std::string {Test::ROOT} + DATA_FILENAME, test.db.info().page_size()};
//            const auto page = inspector.get_page(PageId::root());
//            hexdump(page.view(0).data(), page.size());
//            fmt::print("\n");
//        } else {
            EXPECT_TRUE(expose_message(test.db.commit()));
            committed.insert(cend(committed), cbegin(temp), cend(temp));
//        }
    }
    return committed;
}

TEST_F(XactTests, SanityCheck_1)
{
    for (const auto &r: run_random_transactions(*this, 1)) {
        ASSERT_TRUE(tools::contains(db, r.key));
    }
}

TEST_F(XactTests, SanityCheck_10)
{
    for (const auto &r: run_random_transactions(*this, 10)) {
        ASSERT_TRUE(tools::contains(db, r.key));
    }
}

// TODO
//TEST_F(XactTests, PersistenceSanityCheck)
//{
//    for (Size i {}; i < 10; ++i) {
//        for (const auto &r: run_random_transactions(*this, 10)) {
//            ASSERT_TRUE(tools::contains(db, r.key));
//        }
//    }
//}
//
//class IncompleteXactTests: public TestOnHeap {
//public:
//    static constexpr Size PAGE_SIZE {0x200};
//
//    IncompleteXactTests() = default;
//
//    ~IncompleteXactTests() override = default;
//
//    auto SetUp() -> void override
//    {
//        WriteAheadLog *temp {};
//        BasicWriteAheadLog::Parameters param {
//            ROOT,
//            store.get(),
//            create_sink(),
//            PAGE_SIZE,
//        };
//        ASSERT_TRUE(expose_message(BasicWriteAheadLog::open(param, &temp)));
//        wal.reset(temp);
//
//        options.page_size = PAGE_SIZE;
//        options.frame_count = 16;
//        options.log_level = spdlog::level::trace;
//        options.store = store.get();
//        options.wal = wal.get();
//
//        ASSERT_TRUE(expose_message(db.open(ROOT, options)));
//        ASSERT_TRUE(db.is_open());
//
//        committed = run_random_transactions(*this, 10);
//
//
//        (void)db.close();
//        wal.reset();
//
//        store.reset(dynamic_cast<HeapStorage &>(*store).clone());
//        param.store = store.get();
//
//        ASSERT_TRUE(expose_message(BasicWriteAheadLog::open(param, &temp)));
//        wal.reset(temp);
//        options.store = store.get();
//        options.wal = wal.get();
//
//        ASSERT_TRUE(expose_message(db.open(ROOT, options)));
//        ASSERT_TRUE(db.is_open());
//
//        for (const auto &[key, value]: committed)
//            tools::expect_contains(db, key, value);
//
//        ASSERT_EQ(db.info().record_count(), committed.size());
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
//    std::vector<Record> committed;
//    Random random {123};
//    Options options;
//    Database db;
//};
//
//TEST_F(IncompleteXactTests, X)
//{
//
//}

} // <anonymous>